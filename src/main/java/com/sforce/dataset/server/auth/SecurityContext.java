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


package com.sforce.dataset.server.auth;

import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;

import java.io.Serializable;
import java.util.Calendar;

public final class SecurityContext implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String orgId;
    private String userId;
    private String endPoint;
    private String endPointHost;
    private String sessionId;
    private String userName;
    private String language;
    private String locale;
    private String timeZone;
    private String role;
    private transient PartnerConnection connection;
    private Calendar lastRefreshTimestamp;
    
    public void init(GetUserInfoResult userInfo) {
        this.orgId = userInfo.getOrganizationId();
        this.userId = userInfo.getUserId();
        this.userName = userInfo.getUserName();
        this.language = userInfo.getUserLanguage();
        this.locale = userInfo.getUserLocale();
        this.timeZone = userInfo.getUserTimeZone();
    }
    
    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }
    
    public String getOrgId() {
        return orgId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public String getEndPointHost() {
        return endPointHost;
    }

    public void setEndPointHost(String endPointHost) {
        this.endPointHost = endPointHost;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
     public void setUserName(String userName) {
        this.userName = userName;
    }
    
    public String getUserName() {
        return userName;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
    
    public String getLanguage() {
        return language;
    }
    
    public void setLocale(String locale) {
        this.locale = locale;
    }
    
    public String getLocale() {
        return locale;
    }
    
    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }
    
    public String getTimeZone() {
        return timeZone;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }
    
    public SecurityContext getSecurityContext() {
        return this;
    }

	public PartnerConnection getConnection() {
		return connection;
	}

	public void setConnection(PartnerConnection connection) {
		this.connection = connection;
	}

	public Calendar getLastRefreshTimestamp() {
		return lastRefreshTimestamp;
	}

	public void setLastRefreshTimestamp(Calendar lastRefreshTimestamp) {
		this.lastRefreshTimestamp = lastRefreshTimestamp;
	}
}

