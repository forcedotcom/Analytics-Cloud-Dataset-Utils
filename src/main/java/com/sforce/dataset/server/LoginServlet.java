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
package com.sforce.dataset.server;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.scheduler.SchedulerUtil;
import com.sforce.dataset.server.auth.AuthFilter;
import com.sforce.dataset.server.auth.SecurityContext;
import com.sforce.dataset.server.auth.SecurityContextSessionStore;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.GetServerTimestampResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class LoginServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try
		{
			List<FileItem> items = MultipartRequestHandler.getUploadRequestFileItems(request);
			String userName = request.getParameter("UserName");
			String password = request.getParameter("Password");
			String authEndpoint = request.getParameter("AuthEndpoint");

			for (FileItem item : items) {
				if (item.isFormField()) {
					if (item.getFieldName().equals("UserName")) {
						userName =  item.getString();
					}
					if (item.getFieldName().equals("Password")) {
						password =  item.getString();
					}
					if (item.getFieldName().equals("AuthEndpoint")) {
						authEndpoint =  item.getString();
					}
				}
			}
			
			String sessionId = null;
				    
			if(userName==null || userName.trim().isEmpty())
			{
				throw new IllegalArgumentException("UserName is a required param");
			}

			if(password==null || password.trim().isEmpty())
			{
				throw new IllegalArgumentException("Password is a required param");
			}

			if(authEndpoint==null || authEndpoint.trim().isEmpty())
			{
				throw new IllegalArgumentException("AuthEndpoint is a required param");
			}
			
			if(userName.equals("-1"))
			{
				sessionId = password;
			}
			PartnerConnection conn = DatasetUtils.login(0, userName, password, null, authEndpoint, sessionId, false);
			ConnectorConfig config = conn.getConfig();

			GetServerTimestampResult serverTimestampResult = conn.getServerTimestamp();							

	        SecurityContext sc = new SecurityContext();	        

	        sc.setSessionId(config.getSessionId());
	        sc.setEndPoint(config.getServiceEndpoint());
	        if (config.getServiceEndpoint().indexOf("/services/Soap/u") > 0) {
	            sc.setEndPointHost(config.getServiceEndpoint().substring(0, config.getServiceEndpoint().indexOf("/services/Soap/u")));
	        }

	        
	        sc.init(conn.getUserInfo());

	        SObject[] results;
	        try {
	            results = Connector.newConnection(config).retrieve("Name", "Profile",
	                new String[] {conn.getUserInfo().getProfileId()});
	        } catch (ConnectionException e) {
	            results = null;
	        }
	        
	        String role = null;
	        if (results != null && results.length > 0) {
	            SObject result = results[0];
	            role = (String) result.getField("Name");
	            
	            if (role.isEmpty()) {
	                role = AuthFilter.DEFAULT_ROLE;
	            }
	        } else {
	            role = AuthFilter.DEFAULT_ROLE;
	        }
	        
	        sc.setRole(role);
	        
			if (serverTimestampResult.getTimestamp() != null) {
				sc.setLastRefreshTimestamp(serverTimestampResult.getTimestamp());
				sc.setConnection(conn);
			}
	        
	        SecurityContextSessionStore scStore = new SecurityContextSessionStore();
	        scStore.storeSecurityContext(request, sc);
	        
	        try
	        {
	        	SchedulerUtil.startAllSchedules(conn);
	        }catch(Exception e)
	        {
	        	e.printStackTrace();
	        }
	        
			ResponseStatus status = new ResponseStatus("success",null);
			response.setContentType("application/json");
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
			// Set standard HTTP/1.1 no-cache headers.
			response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
			// Set standard HTTP/1.0 no-cache header.
			
			response.setHeader("Pragma", "no-cache");
		}catch(Throwable t)
		{
			response.setContentType("application/json");
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			ResponseStatus status = new ResponseStatus("error",t.getMessage());
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
		}
	}	
}
