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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.sforce.dataset.DatasetUtilConstants;

public class Session {
	
static final LinkedList<Session> q = new LinkedList<Session>();


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
volatile AtomicBoolean isDone = new AtomicBoolean(false);

Map<String,String> params = new LinkedHashMap<String,String>();
private SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

public Session(String orgId,String name) {
	super();
	if(orgId == null || name == null || orgId.trim().isEmpty() || name.trim().isEmpty())
	{
		throw new IllegalArgumentException("Input arguments (orgId, name) cannot be null");
	}
	this.name = name;
	this.id = UUID.randomUUID().toString();
	this.sessionLog = new File(DatasetUtilConstants.getLogsDir(orgId),name+"_"+id+".log");
	this.orgId = orgId;
	this.status = "INIT";
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
	updateLastModifiedTime();
}

public long getSourceErrorRowCount() {
	return sourceErrorRowCount;
}

public void setSourceErrorRowCount(long sourceErrorRowCount) {
	this.sourceErrorRowCount = sourceErrorRowCount;
	updateLastModifiedTime();
}

public long getTargetTotalRowCount() {
	return targetTotalRowCount;
}

public void setTargetTotalRowCount(long targetTotalRowCount) {
	this.targetTotalRowCount = targetTotalRowCount;
	updateLastModifiedTime();
}

public long getTargetErrorCount() {
	return targetErrorCount;
}

public void setTargetErrorCount(long targetErrorCount) {
	this.targetErrorCount = targetErrorCount;
	updateLastModifiedTime();
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
	updateLastModifiedTime();
}

public String getMessage() {
	return message;
}

public void start() {
	this.status = "STARTED";
	updateLastModifiedTime();
	this.startTime = this.lastModifiedTime;
}

public void end() {
	this.status = "COMPLETED";
	isDone.set(true);
	updateLastModifiedTime();
	this.endTime = this.lastModifiedTime;
}

public void fail(String message) {
	this.status = "FAILED";
	this.message = message;
	isDone.set(true);
	updateLastModifiedTime();
	this.endTime = this.lastModifiedTime;
}

public void terminate(String message) {
	this.status = "TERMINATED";
	if(message!=null)
		this.message = message;
	else
		this.message = "TERMINATED ON USER REQUEST";

	isDone.set(true);	
	updateLastModifiedTime();
	this.endTime = this.lastModifiedTime;
}

public boolean isDone() {
	return isDone.get();
}


void updateLastModifiedTime()
{
	this.lastModifiedTime = System.currentTimeMillis();
}

public static final LinkedList<Session> listSessions(String orgId)
{
	LinkedList<Session> sessionList = new LinkedList<Session>();
	for(Session s:q)
	{
		if(s.orgId.equals(orgId))
		{
			sessionList.add(s);
		}
	}
	return sessionList;
}

public static final Session getSession(String orgId,String id)
{
	for(Session s:q)
	{
		if(s.orgId.equals(orgId) && s.id.equals(id))
		{
			return s;
		}
	}
	return null;
}

public static final Session getCurrentSession(String orgId,String name)
{
    ThreadContext threadContext = ThreadContext.get();
    Session session = threadContext.getSession();
    if(session == null)
    {
    	if(orgId == null || name == null || orgId.trim().isEmpty() || name.trim().isEmpty())
    	{
    		throw new IllegalArgumentException("Input arguments (orgId, name) cannot be null");
    	}
    	session = new Session(orgId,name);
    	threadContext.setSession(session);
    }
    return session;	
}





}
