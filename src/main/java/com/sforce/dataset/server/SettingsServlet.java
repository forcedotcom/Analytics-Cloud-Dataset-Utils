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
import java.io.OutputStream;
import java.math.BigDecimal;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.Config;
import com.sforce.dataset.DatasetUtilConstants;

public class SettingsServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");

		try 
		{					
			ObjectMapper mapper = new ObjectMapper();
			Config config = DatasetUtilConstants.getSystemConfig();
							response.setContentType("application/json");
					        OutputStream output = response.getOutputStream();
					    	mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), config);
					        output.close();
		 }catch (Throwable t) {
				response.setContentType("application/json");
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				ResponseStatus status = new ResponseStatus("error",t.getMessage());
				ObjectMapper mapper = new ObjectMapper();
				mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), status);
		 }
	}
	
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");

		try
		{
			String type = request.getParameter("type");
				    
			if(type==null || type.trim().isEmpty())
			{
				throw new IllegalArgumentException("type is a required param");
			}
						
			if(type.equalsIgnoreCase("settings"))
			{
				String proxyUsername = request.getParameter("proxyUsername");
				if(proxyUsername==null || proxyUsername.trim().isEmpty())
				{
					proxyUsername = null;
				}

				String proxyPassword = request.getParameter("proxyPassword");
				if(proxyPassword==null || proxyPassword.trim().isEmpty())
				{
					proxyPassword = null;
				}

				String proxyNtlmDomain = request.getParameter("proxyNtlmDomain");
				if(proxyNtlmDomain==null || proxyNtlmDomain.trim().isEmpty())
				{
					proxyNtlmDomain = null;
				}

				String proxyHost = request.getParameter("proxyHost");
				if(proxyHost==null || proxyHost.trim().isEmpty())
				{
					proxyHost = null;
				}

				int proxyPort=0;
				String temp = request.getParameter("proxyPort");
				if(temp==null || temp.trim().isEmpty())
				{
//					throw new IllegalArgumentException("interval is a required param");
				}else
				{
					try
					{
						proxyPort = (new BigDecimal(temp.trim())).intValue();
					}catch(Throwable t)
					{
						t.printStackTrace();
					}
					if(proxyPort < 0)
						proxyPort = 0;
				}
				
				int timeoutSecs=540;
				temp = request.getParameter("timeoutSecs");
				if(temp==null || temp.trim().isEmpty())
				{
//					throw new IllegalArgumentException("interval is a required param");
				}else
				{
					try
					{
						timeoutSecs = (new BigDecimal(temp.trim())).intValue();
					}catch(Throwable t)
					{
						t.printStackTrace();
					}
					if(timeoutSecs<60)
						timeoutSecs = 60;
				}

				int connectionTimeoutSecs=60;
				temp = request.getParameter("connectionTimeoutSecs");
				if(temp==null || temp.trim().isEmpty())
				{
//					throw new IllegalArgumentException("interval is a required param");
				}else
				{
					try
					{
						connectionTimeoutSecs = (new BigDecimal(temp.trim())).intValue();
					}catch(Throwable t)
					{
						t.printStackTrace();
					}
					if(connectionTimeoutSecs<60)
						connectionTimeoutSecs = 60;
				}

				boolean noCompression = false;
				temp = request.getParameter("noCompression");
				if(temp!=null && !temp.trim().isEmpty() && temp.equalsIgnoreCase("true"))
				{
					noCompression = true;
				}

				boolean debugMessages = false;
				temp = request.getParameter("debugMessages");
				if(temp!=null && !temp.trim().isEmpty() && temp.equalsIgnoreCase("true"))
				{
					debugMessages = true;
				}
								
				DatasetUtilConstants.setSystemConfig(proxyUsername, proxyPassword, proxyNtlmDomain, proxyHost, proxyPort, timeoutSecs, connectionTimeoutSecs, noCompression, debugMessages);				
			}else
			{
				response.setContentType("application/json");
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				ResponseStatus status = new ResponseStatus("error","Invalid Request {"+type+"}");
				ObjectMapper mapper = new ObjectMapper();
				mapper.writeValue(response.getOutputStream(), status);
			}
			
			ResponseStatus status = new ResponseStatus("success",null);
			response.setContentType("application/json");
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
		}catch(Throwable t)
		{
			response.setContentType("application/json");
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			ResponseStatus status = new ResponseStatus("error",t.getMessage()!=null?t.getMessage():t.toString());
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
		}
	}
	
}
