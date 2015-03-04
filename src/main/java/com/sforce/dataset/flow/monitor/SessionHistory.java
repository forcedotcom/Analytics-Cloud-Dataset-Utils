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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class SessionHistory implements Comparable<SessionHistory> {
	
static final LinkedList<SessionHistory> q = new LinkedList<SessionHistory>();


long startTime = 0l;
long endTime= 0l;
long lastModifiedTime = 0l;
String name = null;
String id = null;
File sessionLog = null;
String orgId = null;
long sourceTotalRowCount=0;
long sourceErrorRowCount=0;
long targetTotalRowCount=0;
long targetErrorCount=0;
String status;
String message = "";
String workflowId = null;
String jobTrackerid = null;
volatile AtomicBoolean isDone = new AtomicBoolean(false);
volatile AtomicBoolean nodeDetailsFetched = new AtomicBoolean(false);

Map<String,String> params = new LinkedHashMap<String,String>();
private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

public SessionHistory(Session session) {
	this.endTime = session.endTime;
	this.id = session.id;
	this.isDone = session.isDone;
	this.lastModifiedTime = session.lastModifiedTime;
	this.message = session.message;
	this.name = session.name;
	this.orgId = session.orgId;
	this.params.putAll(session.params);
	this.sessionLog = session.sessionLog;
	this.sourceErrorRowCount = session.sourceErrorRowCount;
	this.sourceTotalRowCount = session.sourceTotalRowCount;
	this.startTime = session.startTime;
	this.status = session.status;
	this.targetErrorCount = session.targetErrorCount;
	this.targetTotalRowCount = session.targetTotalRowCount;
	this.workflowId = session.workflowId;
	q.add(this);
}

public SessionHistory(String orgId,JobEntry job) {
	super();
	if(orgId == null || job == null  || job._uid == null ||  orgId.trim().isEmpty() ||   job._uid.isEmpty())
	{
		throw new IllegalArgumentException("Input arguments (orgId, job) cannot be null");
	}
	this.id = UUID.randomUUID().toString();
	this.orgId = orgId;
	this.jobTrackerid = job._uid;
	this.endTime = job.endTimeEpoch;
	this.startTime = job.startTimeEpoch;
	this.name = job.workflowName.replace(" upload flow", "");
	this.message = job.errorMessage;
	if(job.endTimeEpoch==0)
		this.lastModifiedTime = job.startTimeEpoch;
	else
		this.lastModifiedTime = job.endTimeEpoch;
	if(job.status==2)
	{
		this.status = "RUNNING";
	}
	else if(job.status==1)
	{
		this.status = "COMPLETED";
		isDone.set(true);
	}else
	{
		this.status = "FAILED";
		isDone.set(true);
	}
	q.add(this);
}


public Map<String, String> getParams() {
	return params;
}

public void setParams(Map<String, String> params) {
	this.params = params;
}

@JsonIgnore
public void setParam(String key,String value)
{
	if(key!=null && !key.isEmpty())
		params.put(key, value);
}

@JsonIgnore
public String getParam(String key)
{
	if(key!=null && !key.isEmpty())
		return params.get(key);
	return null;
}


public long getSourceTotalRowCount() {
	return sourceTotalRowCount;
}

public void setSourceTotalRowCount(long sourceTotalRowCount) {
	this.sourceTotalRowCount = sourceTotalRowCount;
}

public long getSourceErrorRowCount() {
	return sourceErrorRowCount;
}

public void setSourceErrorRowCount(long sourceErrorRowCount) {
	this.sourceErrorRowCount = sourceErrorRowCount;
}

public long getTargetTotalRowCount() {
	return targetTotalRowCount;
}

public void setTargetTotalRowCount(long targetTotalRowCount) {
	this.targetTotalRowCount = targetTotalRowCount;
}

public long getTargetErrorCount() {
	return targetErrorCount;
}

public void setTargetErrorCount(long targetErrorCount) {
	this.targetErrorCount = targetErrorCount;
}


public long getStartTime() {
	return startTime;
}

public String getStartTimeFormatted() {
	if(startTime != 0)
		return sdf.format(new Date(startTime));
	else
		return null;
}

public long getEndTime() {
	return endTime;
}

public String getEndTimeFormatted() {
	if(endTime != 0)
		return sdf.format(new Date(endTime));
	else
		return null;
}

public String getName() {
	return name;
}

public void setName(String name) {
	this.name = name;
}


public String getId() {
	return id;
}

public File getSessionLog() {
	return sessionLog;
}

public String getOrgId() {
	return orgId;
}

public String getStatus() {
	return status;
}

public void setStatus(String status) {
	this.status = status;
}

public void setStartTime(long startTime) {
	this.startTime = startTime;
}


public void setEndTime(long endTime) {
	this.endTime = endTime;
	updateLastModifiedTime(endTime);
}


public void setMessage(String message) {
	this.message = message;
}


public String getMessage() {
	return message;
}

public String getWorkflowId() {
	return workflowId;
}

public void setWorkflowId(String workflowId) {
	this.workflowId = workflowId;
}

public String getJobTrackerid() {
	return jobTrackerid;
}

public void setJobTrackerid(String jobTrackerid) {
	this.jobTrackerid = jobTrackerid;
}

public boolean isDone() {
	return isDone.get();
}

public boolean isNodeDetailsFetched() {
	return nodeDetailsFetched.get();
}

public void setNodeDetailsFetched(boolean newValue) {
	this.nodeDetailsFetched.set(newValue);
}

public void updateLastModifiedTime(long updatedTime)
{
	if(updatedTime==0)
		this.lastModifiedTime = System.currentTimeMillis();
	else
		this.lastModifiedTime = updatedTime;
}

public static final LinkedList<SessionHistory> listSessions(String orgId)
{
	LinkedList<SessionHistory> sessionList = new LinkedList<SessionHistory>();
	long sevenDaysAgo = System.currentTimeMillis() - 7*24*60*60*1000;
	List<SessionHistory> copy = new LinkedList<SessionHistory>(q);
	for(SessionHistory s:copy)
	{
		if(s.lastModifiedTime > sevenDaysAgo)
		{
			if(s.orgId.equals(orgId))
			{
					sessionList.add(s);
			}
		}else
		{
			q.remove(s);
		}
	}
	Collections.sort(sessionList, Collections.reverseOrder());
	return sessionList;
}

public static final SessionHistory getSession(String orgId,String id)
{
	for(SessionHistory s:q)
	{
		if(s.orgId.equals(orgId) && s.id.equals(id))
		{
			return s;
		}
	}
	return null;
}

public static final SessionHistory getSessionByJobTrackerId(String orgId,String id)
{
	if(id==null||id.trim().isEmpty())
		return null;
	
	for(SessionHistory s:q)
	{
		if(s.orgId.equals(orgId) && id.equals(s.jobTrackerid))
		{
			return s;
		}
	}
	return null;
}



@Override
public int compareTo(SessionHistory o) {
	if (this.lastModifiedTime > o.lastModifiedTime)
		return 1;
	else if (this.lastModifiedTime < o.lastModifiedTime)
		return -1;
	else
		return 0;
}

}
