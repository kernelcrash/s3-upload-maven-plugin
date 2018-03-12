package com.bazaarvoice.maven.plugins.s3.upload;

import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.GroupGrantee;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.Serializable;

public class Permission implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * AWS Grantee.
	 */
	@Parameter
	private String grantee;

	@Parameter(defaultValue = "true")
	private Boolean download;

	@Parameter(defaultValue = "false")
	private Boolean viewPermission = Boolean.FALSE;

	@Parameter(defaultValue = "false")
	private Boolean editPermission = Boolean.FALSE;

	public String getGrantee() {
		return grantee;
	}

	public void setGrantee(String grantee) {
		this.grantee = grantee;
	}

	public Boolean getDownload() {
		return download;
	}

	public void setDownload(Boolean download) {
		this.download = download;
	}

	public Boolean getViewPermission() {
		return viewPermission;
	}

	public void setViewPermission(Boolean viewPermission) {
		this.viewPermission = viewPermission;
	}

	public Boolean getEditPermission() {
		return editPermission;
	}

	public void setEditPermission(Boolean editPermission) {
		this.editPermission = editPermission;
	}

	public Grantee getAsGrantee() {
		if (grantee.equalsIgnoreCase("Everyone") || grantee.equalsIgnoreCase("allusers")) {
			return GroupGrantee.AllUsers;
		}
		if (grantee.equalsIgnoreCase("AuthenticatedUsers")) {
			return GroupGrantee.AuthenticatedUsers;
		}
		if (grantee.equalsIgnoreCase("LogDelivery")) {
			return GroupGrantee.LogDelivery;
		}
		if (grantee.matches("@[^.]+\\.[^.]+")) {
			return new EmailAddressGrantee(grantee);
		}
		return new CanonicalGrantee(grantee);
	}

	public com.amazonaws.services.s3.model.Permission getPermission() {
		if (download && editPermission && viewPermission) {
			return com.amazonaws.services.s3.model.Permission.FullControl;
		} else {
			if (editPermission) {
				return com.amazonaws.services.s3.model.Permission.WriteAcp;
			} else {
				if (viewPermission) {
					return com.amazonaws.services.s3.model.Permission.ReadAcp;
				} else {
					return com.amazonaws.services.s3.model.Permission.Read;
				}
			}
		}
	}
}
