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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.fileupload.FileItem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.DataFlowMonitorUtil;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.flow.monitor.SessionHistory;
import com.sforce.dataset.server.auth.AuthFilter;
import com.sforce.soap.partner.PartnerConnection;

@MultipartConfig 
public class FileUploadServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private static final int MAX_THREAD_POOL = 20;
	private static final ThreadPoolExecutor executorPool = new ThreadPoolExecutor(2, 2, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(MAX_THREAD_POOL), Executors.defaultThreadFactory());
//	private static List<FileUploadRequest> files = new LinkedList<FileUploadRequest>();

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try
		{
			// Set standard HTTP/1.1 no-cache headers.
			response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
			// Set standard HTTP/1.0 no-cache header.
			response.setHeader("Pragma", "no-cache");
			
		    response.setContentType("application/json");

			PartnerConnection conn = AuthFilter.getConnection(request);
			if(conn==null)
			{
			   	response.sendRedirect(request.getContextPath() + "/login.html");
			   	return;
			}
			
			if(executorPool.getQueue().size()>=MAX_THREAD_POOL)
			{
		    	response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "There are too many jobs in the queue, please try again later");
		    	return;
			}
			
			String orgId = conn.getUserInfo().getOrganizationId();
			File dataDir = DatasetUtilConstants.getDataDir(orgId);
			List<FileItem> items = MultipartRequestHandler.getUploadRequestFileItems(request);
			Session session = new Session(orgId,MultipartRequestHandler.getDatasetName(items));
			List<FileUploadRequest> files = (MultipartRequestHandler.uploadByApacheFileUpload(items, dataDir,session));
			CsvUploadWorker worker = new CsvUploadWorker(files, conn, session);
			
		    try
		    {
		    	executorPool.execute(worker);
		    }catch(Throwable t)
		    {
		    	Session.removeCurrentSession();
		    	response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "There are too many jobs in the queue, please try again later");
		    	return;
		    }

			ResponseStatus status = new ResponseStatus("success",null);
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
		}catch(Throwable t)
		{
	    	Session.removeCurrentSession();
			response.setContentType("application/json");
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			ResponseStatus status = new ResponseStatus("error",t.getMessage());
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
		}
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try 
		{		
			// Set standard HTTP/1.1 no-cache headers.
			response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
			// Set standard HTTP/1.0 no-cache header.
			response.setHeader("Pragma", "no-cache");

			String id = request.getParameter("id");
			String type = request.getParameter("type");
					
				if(type==null || type.trim().isEmpty())
				{
					throw new IllegalArgumentException("Parameter 'type' not found in Request");
				}
				
				if(id==null || id.trim().isEmpty())
				{
					throw new IllegalArgumentException("Parameter 'id' not found in Request");
				}
				
				PartnerConnection conn = AuthFilter.getConnection(request);
				if(conn==null)
				{
			    	response.sendRedirect(request.getContextPath() + "/login.html");
			    	return;
				}
					
				String orgId = conn.getUserInfo().getOrganizationId();
				Session session = Session.getSession(orgId,id);
				SessionHistory sessionHistory =  SessionHistory.getSession(orgId, id);
				if(session == null && sessionHistory==null)
				{
						throw new IllegalArgumentException("Invalid 'id' {"+id+"}");
				}

				File file = null;
				String contentType = null;
				if(type.equalsIgnoreCase("errorCsv"))
				{
					contentType = "text/csv";
					if(session!=null)
					{
						Map<String, String> params = session.getParams();
						String errorFile = params.get(DatasetUtilConstants.errorCsvParam);
						if(errorFile != null)
						{
							file = new File(errorFile);
						}
					}else
					{
						file = DataFlowMonitorUtil.getJobErrorFile(conn, sessionHistory.getName(), sessionHistory.getJobTrackerid());
					}
				}else if(type.equalsIgnoreCase("metadataJson"))
				{
					contentType = "application/json";
					Map<String, String> params = session!=null?session.getParams():sessionHistory.getParams();
					String errorFile = params.get(DatasetUtilConstants.metadataJsonParam);
					if(errorFile != null)
					{
						file = new File(errorFile);
					}
				}else if(type.equalsIgnoreCase("sessionLog"))
				{
					contentType = "text/plain";
					file = session.getSessionLog();
				}else if(type.equalsIgnoreCase("terminateSession"))
				{
					session.terminate(null);
				    response.setContentType("application/json");
			    	ObjectMapper mapper = new ObjectMapper();
					ResponseStatus status = new ResponseStatus("success",null);
			    	mapper.writeValue(response.getOutputStream(), status);
			    	return;
				}else
				{
					throw new IllegalArgumentException("Invalid 'type' {"+type+"}");
				}
					
				if(file!=null && file.exists())
				{
					response.setContentType(contentType);
					response.setHeader("Content-disposition", "attachment; filename=\""+file.getName()+"\"");
					InputStream input = new FileInputStream(file);
			        OutputStream output = response.getOutputStream();
			        IOUtils.copy(input, output);
			        output.close();
			        input.close();
				}else
				{
					throw new FileNotFoundException("File " + type + " not found");
				}
		 }catch (Throwable t) {
				response.setContentType("application/json");
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				ResponseStatus status = new ResponseStatus("error",t.getMessage());
				ObjectMapper mapper = new ObjectMapper();
				mapper.writeValue(response.getOutputStream(), status);
		 }
	}
}
