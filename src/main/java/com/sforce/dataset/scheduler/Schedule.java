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
package com.sforce.dataset.scheduler;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class Schedule {
	
	private String devName;
	private String masterLabel;
	private String cronSchedule;
	private boolean disabled = true;

	private String jobType = "dataflow";
	private Map<?,?> jobDataMap;
	private UserType lastModifiedBy;
	private long scheduleStartDate = 0;
	private long nextRunTime = 0;
	private String frequency;
	private long interval = 0;

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

	public long getScheduleStartDate() {
		return scheduleStartDate;
	}
	public void setScheduleStartDate(long scheduleStartDate) {
		this.scheduleStartDate = scheduleStartDate;
	}
	public String getFrequency() {
		return frequency;
	}
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	public long getInterval() {
		return interval;
	}
	public void setInterval(long interval) {
		this.interval = interval;
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
	public String getCronSchedule() {
		return cronSchedule;
	}
	public void setCronSchedule(String cronSchedule) {
		this.cronSchedule = cronSchedule;
	}
	public boolean isDisabled() {
		return disabled;
	}
	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}
	public Map<?,?> getJobDataMap() {
		return jobDataMap;
	}
	public void setJobDataMap(Map<?,?> jobDataMap) {
		this.jobDataMap = jobDataMap;
	}		
	public String getJobType() {
		if(jobType==null)
			jobType = "dataflow";
		return jobType;
	}
	public void setJobType(String jobType) {
		this.jobType = jobType;
	}
	public UserType getLastModifiedBy() {
		return lastModifiedBy;
	}
	public void setLastModifiedBy(UserType lastModifiedBy) {
		this.lastModifiedBy = lastModifiedBy;
	}
	public long getNextRunTime() {
		return nextRunTime;
	}
	public void setNextRunTime(long nextRunTime) {
		this.nextRunTime = nextRunTime;
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
