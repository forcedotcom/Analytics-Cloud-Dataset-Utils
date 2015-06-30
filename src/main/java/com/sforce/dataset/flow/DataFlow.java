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
package com.sforce.dataset.flow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;


public class DataFlow {
	
	private String _type = null;
	private UserType _lastModifiedBy;
	private String nextRun;
	private long nextRunTime = 0L;
	private String _url;
	private String name;
	private String MasterLabel;
	private int RefreshFrequencySec = 0;
	private String _uid;
	private String WorkflowType;
	private String status;

	public String getStatus() {
		if(status==null||status.trim().isEmpty())
			status = "Active";
		return status;
	}

	public void setStatus(String status) {
		if(status!=null && !status.trim().isEmpty())
			this.status = status;
	}

	@SuppressWarnings("rawtypes")
	private Map workflowDefinition;

	public String get_type() {
		return _type;
	}

	public void set_type(String _type) {
		this._type = _type;
	}

	public UserType get_lastModifiedBy() {
		return _lastModifiedBy;
	}

	public void set_lastModifiedBy(UserType _lastModifiedBy) {
		this._lastModifiedBy = _lastModifiedBy;
	}

	@JsonIgnore
	void setLastModifiedBy(PartnerConnection partnerConnection) throws ConnectionException {
		UserType ut = new UserType();
		GetUserInfoResult ui = partnerConnection.getUserInfo();
		ut._uid = ui.getUserId();
		ut.name = ui.getUserFullName();
		ut._type = "user";
		this._lastModifiedBy = ut;
	}

	
	public String getNextRun() {
		return nextRun;
	}

	public void setNextRun(String nextRun) {
		this.nextRun = nextRun;
	}

	public long getNextRunTime() {
		return nextRunTime;
	}

	public void setNextRunTime(long nextRunTime) {
		this.nextRunTime = nextRunTime;
	}

	public String get_url() {
		return _url;
	}

	public void set_url(String _url) {
		this._url = _url;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMasterLabel() {
		return MasterLabel;
	}

	public void setMasterLabel(String masterLabel) {
		MasterLabel = masterLabel;
	}

	public int getRefreshFrequencySec() {
		return RefreshFrequencySec;
	}

	public void setRefreshFrequencySec(int refreshFrequencySec) {
		RefreshFrequencySec = refreshFrequencySec;
	}

	public String get_uid() {
		return _uid;
	}

	public void set_uid(String _uid) {
		this._uid = _uid;
	}

	public String getWorkflowType() {
		if(WorkflowType==null || WorkflowType.trim().isEmpty())
			WorkflowType = "Local";
		return WorkflowType;
	}

	public void setWorkflowType(String workflowType) {
		WorkflowType = workflowType;
	}

	public Map getWorkflowDefinition() {
		return workflowDefinition;
	}

	public void setWorkflowDefinition(Map workflowDefinition) {
		this.workflowDefinition = workflowDefinition;
	}

	@Override
	public String toString() {
		return "DataFlow [_type=" + _type + ", _lastModifiedBy="
				+ _lastModifiedBy + ", nextRun=" + nextRun + ", nextRunTime="
				+ nextRunTime + ", _url=" + _url + ", name=" + name
				+ ", MasterLabel=" + MasterLabel + ", RefreshFrequencySec="
				+ RefreshFrequencySec + ", _uid=" + _uid + ", WorkflowType="
				+ WorkflowType + ", workflowDefinition=" + workflowDefinition
				+ "]";
	}

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
    
	public static UserType getUserType(DataFlow obj,Map<String,String> input)
	{
		if(input!=null && !input.isEmpty())
		{
			UserType ret =  new UserType();
			ret._type = (String) input.get("_type");
			ret._uid = (String) input.get("_uid");
			ret.name = (String) input.get("name");
			ret.profilePhotoUrl = (String) input.get("profilePhotoUrl");
			return ret;
		}
		return null;
	}

}
