package com.bazaarvoice.maven.plugins.s3.upload;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;

@Mojo(name = "s3-upload")
public class S3UploadMojo extends AbstractMojo {
	/** Access key for S3. */
	@Parameter(property = "s3-upload.accessKey")
	private String accessKey;

	/** Secret key for S3. */
	@Parameter(property = "s3-upload.secretKey")
	private String secretKey;

	/**
	 * Execute all steps up except the upload to the S3. This can be set to true
	 * to perform a "dryRun" execution.
	 */
	@Parameter(property = "s3-upload.doNotUpload", defaultValue = "false")
	private boolean doNotUpload;

	/**
	 * Skips execution
	 */
	@Parameter(property = "s3-upload.skip", defaultValue = "false")
	private boolean skip;

	/** The file/folder to upload. */
	@Parameter(property = "s3-upload.source", required = true)
	private String source;

	/** The bucket to upload into. */
	@Parameter(property = "s3-upload.bucketName", required = true)
	private String bucketName;

	/** The file/folder (in the bucket) to create. */
	@Parameter(property = "s3-upload.destination", required = true)
	private String destination;

	/** Force override of endpoint for S3 regions such as EU. */
	@Parameter(property = "s3-upload.endpoint")
	private String endpoint;

	/** In the case of a directory upload, recursively upload the contents. */
	@Parameter(property = "s3-upload.recursive", defaultValue = "false")
	private boolean recursive;

	@Parameter(property = "s3-upload.permissions")
	private LinkedList<Permission> permissions;

	@Parameter(property = "s3-upload.metadatas")
	private LinkedList<Metadata> metadatas;

	@Parameter(property = "s3-upload.pool")
	private Integer executorPoolSize = 10;

	private ExecutorService executorService;

	@Override
	public void execute() throws MojoExecutionException {
		if (skip) {
			getLog().info("Skipping S3UPload");
			return;
		}

		File sourceFile = new File(source);
		if (!sourceFile.exists()) {
			throw new MojoExecutionException("File/folder doesn't exist: " + source);
		}

		if ("/".equals(destination)) {
			destination = "";
		}

		AmazonS3 s3 = getS3Client(accessKey, secretKey);
		if (endpoint != null) {
			s3.setEndpoint(endpoint);
		}

		if (!s3.doesBucketExist(bucketName)) {
			throw new MojoExecutionException("Bucket doesn't exist: " + bucketName);
		}

		if (doNotUpload) {
			getLog().info(String.format("File %s would have be uploaded to s3://%s/%s (dry run)", sourceFile, bucketName, destination));

			return;
		}

		executorService = Executors.newFixedThreadPool(executorPoolSize);

    boolean success = upload(s3, sourceFile);

    executorService.shutdown();
    // now wait for them to finish
    try {
      executorService.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      getLog().error(e);
      success = false;
    }

    if (!success) {
			throw new MojoExecutionException("Unable to upload file to S3.");
		}

		getLog().info(String.format("File %s uploaded to s3://%s/%s", sourceFile, bucketName, destination));
	}

	private static AmazonS3 getS3Client(String accessKey, String secretKey) {
		AWSCredentialsProvider provider;
		if (accessKey != null && secretKey != null) {
			AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
			provider = new StaticCredentialsProvider(credentials);
		} else {
			provider = new DefaultAWSCredentialsProviderChain();
		}

		return new AmazonS3Client(provider);
	}

	private boolean upload(AmazonS3 s3, File sourceFile) throws MojoExecutionException {
		TransferManager mgr = new TransferManager(s3);

		Transfer transfer;
		if (sourceFile.isFile()) {
			transfer = mgr.upload(bucketName, destination, sourceFile);
		} else if (sourceFile.isDirectory()) {
			transfer = mgr.uploadDirectory(bucketName, destination, sourceFile, recursive);
		} else {
			throw new MojoExecutionException("File is neither a regular file nor a directory " + sourceFile);
		}
		try {
			getLog().info("Transferring " + sourceFile.getAbsolutePath() + " : " + transfer.getProgress().getTotalBytesToTransfer() + " bytes...");
			transfer.waitForCompletion();
			getLog().info("Transferred " + transfer.getProgress().getBytesTransfered() + " bytes.");

			try {
				if(metadatas != null && metadatas.size() > 0){
					updateMetadatas(s3, sourceFile,sourceFile.getCanonicalPath(),destination, 0);
				}
				if(permissions != null && permissions.size() > 0){
					updatePermissions(s3,sourceFile,sourceFile.getCanonicalPath(),destination, 0);
				}
			} catch (IOException e) {
				throw new MojoExecutionException("Error getting file canonicalPath when updating permissions/metadatas",e);
			}
		} catch (InterruptedException e) {
			return false;
		}

		return true;
	}

	private void updatePermissions(AmazonS3 s3, File sourceFile, String localPrefix, String keyPrefix, Integer folderLevel) throws MojoExecutionException {
		try{
			if (sourceFile.isFile()) {
				updatePermissions(s3, sourceFile.getCanonicalPath().replace(localPrefix, keyPrefix));
			} else {
				//this will allow first level folder, but not following
				if(recursive || folderLevel <= 0){
					for(File f:sourceFile.listFiles()){
						updatePermissions(s3, f, f.getCanonicalPath(), keyPrefix.replaceAll("([^"+File.separatorChar+"])"+File.separatorChar+"*$","$1"+File.separatorChar)+f.getName(), folderLevel + 1);
					}
				}
			}
		}catch(IOException ioe){
			throw new MojoExecutionException("Error getting file canonicalPath when updating permissions",ioe);
		}
	}
	private void updatePermissions(final AmazonS3 s3, final String key) {
    getLog().info("submitting updating permissions for key: "+key);
	  executorService.submit(new Runnable() {
      @Override
      public void run() {
        getLog().info("Updating permissions for key: "+key);
        AccessControlList acl = s3.getObjectAcl(bucketName, key);
        for(Permission p :permissions){
          acl.grantPermission(p.getAsGrantee(), p.getPermission());
        }
        s3.setObjectAcl(bucketName, key, acl);
//		getLog().info("Updating permissions for '"+key+"' in bucket '"+bucketName);
      }
    });
	}

	private void updateMetadatas(AmazonS3 s3, File sourceFile, String localPrefix, String keyPrefix, Integer folderLevel) throws MojoExecutionException {
		try{
			if (sourceFile.isFile()) {
				updateMetadatas(s3, sourceFile.getCanonicalPath().replace(localPrefix, keyPrefix));
			} else {
				if(recursive || folderLevel <= 0){
					for(File f:sourceFile.listFiles()){
						updateMetadatas(s3, f, f.getCanonicalPath(), keyPrefix.replaceAll("([^/])/*$","$1/")+f.getName(), folderLevel+1);
					}
				}
			}
		}catch(IOException ioe){
			throw new MojoExecutionException("Error getting file canonicalPath when updating metadatas",ioe);
		}
	}

	private void updateMetadatas(final AmazonS3 s3, final String key) throws MojoExecutionException {
    getLog().info("submitting updating Metadata for key: " + key);
	  executorService.submit(new Runnable() {
      @Override
      public void run() {
        List<String> keys = new ArrayList<>();
        for (Metadata m: metadatas) {
          if(m.shouldSetMetadata(key)){
            keys.add(String.format("%s=%s", m.getKey(), m.getValue()));
          }
        }

        getLog().info("Updating Metadata for key: " + key + " (" + keys.stream().collect(Collectors.joining(",")) + ")");

        S3Object s3o = s3.getObject(bucketName, key);
        for (Metadata m: metadatas) {
          if(m.shouldSetMetadata(key)){
            s3o.getObjectMetadata().setHeader(m.getKey(), m.getValue());
          }
        }
        s3.putObject(bucketName, key, s3o.getObjectContent(), s3o.getObjectMetadata());
      }
    });
	}
}
