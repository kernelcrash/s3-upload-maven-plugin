package com.bazaarvoice.maven.plugins.s3.upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.util.IOUtils;
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

	@Parameter(property = "s3-upload.publicRead", defaultValue = "false")
	private Boolean publicRead = Boolean.FALSE;

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

	@Parameter(property = "s3-only-new", defaultValue = "true")
	private boolean onlyUploadNew;

	@Parameter(property = "s3-upload.permissions")
	private LinkedList<Permission> permissions;

	@Parameter(property = "s3-upload.metadatas")
	private LinkedList<Metadata> metadatas;

	@Parameter(property = "s3-upload.pool")
	private Integer executorPoolSize = 10;

	private boolean uploadingSuccessful = true; // by default
	private int copyingFileCount = 0;

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

		if (onlyUploadNew) {
			sourceFile = discoverExistingFilenames(s3, sourceFile);
			if (copyingFileCount == 0) {
				getLog().info("no files to upload");
				return;
			}
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

    if (!success && uploadingSuccessful) {
			throw new MojoExecutionException("Unable to upload file to S3.");
		}

		getLog().info(String.format("File %s uploaded to s3://%s/%s", sourceFile, bucketName, destination));
	}

	private File discoverExistingFilenames(AmazonS3 s3, File sourceFile) throws MojoExecutionException {
		List<String> existingFilenames = new ArrayList<>();

		boolean incomplete = true;
		ObjectListing bucketObjects = s3.listObjects(bucketName, destination);
		while (incomplete) {

			bucketObjects.getObjectSummaries().forEach((os) -> {
				existingFilenames.add(os.getKey());
				getLog().info(String.format("Existing key: %s", bucketName, os.getKey()));
			});
			if (bucketObjects.isTruncated()) {
				bucketObjects = s3.listNextBatchOfObjects(bucketObjects);
			} else {
				incomplete = false;
			}
		}

		if (existingFilenames.size() == 0) {
			return sourceFile;
		}

		File target = new File("target/s3-filter");

		target.mkdirs();

		recursiveCopyNewFiles(sourceFile, target, existingFilenames, sourceFile.getPath());

		return target;
	}

	private int recursiveCopyNewFiles(File sourceFile, File target, List<String> existingFilenames, String topPath) throws MojoExecutionException {
		int count = 0;

		for(File f : sourceFile.listFiles()) {
			if (f.getName().equals("..") || f.getName().equals(".")) {

			} else if (f.isDirectory()) {
				File newTarget = new File(target, f.getName());
				newTarget.mkdirs();
				int copiedCount = recursiveCopyNewFiles(f, newTarget, existingFilenames, topPath);
				if (copiedCount == 0) { // if no files were copied under this folder, delete it
					newTarget.delete();
				}
				count += copiedCount;
			} else {
				String relativePath = f.getPath().substring(topPath.length()+1);
				if (existingFilenames.contains(destination + '/' + relativePath)) {
					getLog().info(String.format("skipping %s/%s as exists", destination, relativePath));
				} else {
					File newTarget = new File(target, f.getName());
					copyingFileCount ++;
					count ++;
					getLog().info(String.format("copying %s as new", relativePath));
					try {
						IOUtils.copy(new FileInputStream(f), new FileOutputStream(newTarget));
					} catch (IOException e) {
						getLog().error("Unexpected failure in copy.");
						throw new MojoExecutionException("Unexpected failure copying file", e);
					}
				}
			}
		}

		return count;
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
		TransferManager mgr = TransferManagerBuilder.standard().withS3Client(s3).build();

		Transfer transfer = null;
		if (skip) {
			getLog().info("skipping upload.");
		}
		else if (sourceFile.isFile()) {
			transfer = mgr.upload(bucketName, destination, sourceFile);
		} else if (sourceFile.isDirectory()) {
			transfer = mgr.uploadDirectory(bucketName, destination, sourceFile, recursive);
		} else {
			throw new MojoExecutionException("File is neither a regular file nor a directory " + sourceFile);
		}
		try {
			if (transfer != null) {
				getLog().info("Transferring " + sourceFile.getAbsolutePath() + " : " + transfer.getProgress().getTotalBytesToTransfer() + " bytes...");
				transfer.waitForCompletion();
				getLog().info("Transferred " + transfer.getProgress().getBytesTransfered() + " bytes.");
			}

			try {
				if(metadatas != null && metadatas.size() > 0){
					updateMetadatas(s3, sourceFile,sourceFile.getCanonicalPath(),destination, 0);
				}
				if(permissions != null && permissions.size() > 0){
					updatePermissions(s3,sourceFile,sourceFile.getCanonicalPath(),destination, 0);
				}
				if (publicRead) {
					updatePublicRead(s3,sourceFile,sourceFile.getCanonicalPath(),destination, 0);
				}
			} catch (IOException e) {
				throw new MojoExecutionException("Error getting file canonicalPath when updating permissions/metadatas",e);
			}
		} catch (InterruptedException e) {
			return false;
		}

		return true;
	}

	private void updatePublicRead(AmazonS3 s3, File sourceFile, String localPrefix, String keyPrefix, int folderLevel) throws MojoExecutionException {
		try{
			if (sourceFile.isFile()) {
				updatePublicRead(s3, sourceFile.getCanonicalPath().replace(localPrefix, keyPrefix));
			} else {
				//this will allow first level folder, but not following
				if(recursive || folderLevel <= 0){
					for(File f:sourceFile.listFiles()){
						updatePublicRead(s3, f, f.getCanonicalPath(), keyPrefix.replaceAll("([^"+File.separatorChar+"])"+File.separatorChar+"*$","$1"+File.separatorChar)+f.getName(), folderLevel + 1);
					}
				}
			}
		} catch(IOException ioe){
			throw new MojoExecutionException("Error getting file canonicalPath when updating permissions",ioe);
		}
	}

	private void updatePublicRead(final AmazonS3 s3, final String key) {
		getLog().info("submitting updating public-read for key: "+key);
		executorService.submit(new Runnable() {
			@Override
			public void run() {
				getLog().info("Updating public-read permissions for key: "+key);
				try {
					AccessControlList acl = new AccessControlList();
					acl = s3.getObjectAcl(bucketName, key);
					acl.grantPermission(GroupGrantee.AllUsers, com.amazonaws.services.s3.model.Permission.Read);
					s3.setObjectAcl(bucketName, key, acl);
				} catch (Exception ex) {
					getLog().error("Cannot set permission", ex);
					uploadingSuccessful = false;
				}
			}
		});
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
        try {
	        s3.setObjectAcl(bucketName, key, acl);
        } catch (Exception ex) {
        	getLog().error("Failed to update permissions", ex);
        	uploadingSuccessful = false;
        }
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

        try {
	        S3Object s3o = s3.getObject(bucketName, key);
	        for (Metadata m: metadatas) {
		        if(m.shouldSetMetadata(key)){
			        s3o.getObjectMetadata().setHeader(m.getKey(), m.getValue());
		        }
	        }
	        s3.putObject(bucketName, key, s3o.getObjectContent(), s3o.getObjectMetadata());
        } catch (Exception ex) {
        	getLog().error("Failed to set metadata", ex);
        	uploadingSuccessful = false;
        }
      }
    });
	}
}
