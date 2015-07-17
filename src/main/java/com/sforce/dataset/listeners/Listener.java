/*
 * Copyright (c) 2014, salesforce.com, inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided 
 * that the following conditions are met:
 * 
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the 
 *    following disclaimer.
 *  
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
 *    the following disclaimer in the documentation and/or other materials provided with the distribution. 
 *    
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior written permission.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.sforce.dataset.listeners;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class Listener {
	
	private String devName;
	private String masterLabel;
	private boolean disabled = true;
	private String type;
	private Map<?,?> params;
	private UserType lastModifiedBy;
	private long lastModifiedDate = 0;

	private static class UserType {
        public String _type = null;
        public String profilePhotoUrl = null;
        public String name = null;
        public String _uid = null;
		@Override
		public String toString() {
			return "UserType [_type=" + _type + ", profilePhotoUrl="
					+ profilePhotoUrl + ", name=" + name + ", _uid=" + _uid
					+ "]";
		}
    }

	public String getDevName() {
		return devName;
	}
	public void setDevName(String devName) {
		this.devName = devName;
	}
	public String getMasterLabel() {
		return masterLabel;
	}
	public void setMasterLabel(String masterLabel) {
		this.masterLabel = masterLabel;
	}
	public boolean isDisabled() {
		return disabled;
	}
	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}
	public UserType getLastModifiedBy() {
		return lastModifiedBy;
	}
	public void setLastModifiedBy(UserType lastModifiedBy) {
		this.lastModifiedBy = lastModifiedBy;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Map<?, ?> getParams() {
		return params;
	}
	public void setParams(Map<?, ?> params) {
		this.params = params;
	}
	public long getLastModifiedDate() {
		return lastModifiedDate;
	}
	public void setLastModifiedDate(long lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}
	@JsonIgnore
	void set_LastModifiedBy(PartnerConnection partnerConnection) throws ConnectionException {
		UserType ut = new UserType();
		GetUserInfoResult ui = partnerConnection.getUserInfo();
		ut._uid = ui.getUserId();
		ut.name = ui.getUserFullName();
		ut._type = "user";
		this.setLastModifiedBy(ut);
	}	
}
