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
package com.sforce.dataset.flow.monitor;

public class JobEntry implements Comparable<JobEntry> {

	   public static final Integer SUCCESS = 1;
	   public static final Integer FAILED = 0;
	   public static final Integer RUNNING = 2;
	   public static final Integer QUEUED = 4;
	   public static final Integer WARNING = 5;

	String errorMessage = null;
    long startTimeEpoch = 0;

    int status = 0;
    long endTimeEpoch = 0;
    String _uid = null;
    String type = null;
    String endTime = null;
    String startTime = null;
    String _type = null;
    long duration= 0;
    long _createdDateTime = 0;
    String workflowName = null;
    String nodeUrl = null;
    
	public String getErrorMessage() {
		return errorMessage;
	}
	public long getStartTimeEpoch() {
		return startTimeEpoch;
	}
	public int getStatus() {
		return status;
	}
	public long getEndTimeEpoch() {
		return endTimeEpoch;
	}
	public String get_uid() {
		return _uid;
	}
	public String getType() {
		return type;
	}
	public String getEndTime() {
		return endTime;
	}
	public String getStartTime() {
		return startTime;
	}
	public String get_type() {
		return _type;
	}
	public long getDuration() {
		return duration;
	}
	public long get_createdDateTime() {
		return _createdDateTime;
	}
	public String getWorkflowName() {
		return workflowName;
	}
	public String getNodeUrl() {
		return nodeUrl;
	}
	@Override
	public String toString() {
		return "JobEntry [errorMessage=" + errorMessage + ", startTimeEpoch="
				+ startTimeEpoch + ", status=" + status + ", endTimeEpoch="
				+ endTimeEpoch + ", _uid=" + _uid + ", type=" + type
				+ ", endTime=" + endTime + ", startTime=" + startTime
				+ ", _type=" + _type + ", duration=" + duration
				+ ", _createdDateTime=" + _createdDateTime + ", workflowName="
				+ workflowName + ", nodeUrl=" + nodeUrl + "]";
	}
	
	@Override
	public int compareTo(JobEntry o) {
		if (this.startTimeEpoch > o.startTimeEpoch)
			return 1;
		else if (this.startTimeEpoch < o.startTimeEpoch)
			return -1;
		else
			return 0;
	}
}
