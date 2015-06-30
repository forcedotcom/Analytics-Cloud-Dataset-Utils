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
import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.DataFlow;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.server.auth.AuthFilter;
import com.sforce.dataset.util.DatasetDownloader;
import com.sforce.dataset.util.XmdUploader;
import com.sforce.soap.partner.PartnerConnection;

@MultipartConfig 
public class JsonServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");

		try
		{
			String type = request.getParameter("type");
			String jsonString = request.getParameter("jsonString");
				    
			if(type==null || type.trim().isEmpty())
			{
				throw new IllegalArgumentException("type is a required param");
			}

			if(jsonString==null || jsonString.trim().isEmpty())
			{
				throw new IllegalArgumentException("jsonString is a required param");
			}

			
			PartnerConnection conn = AuthFilter.getConnection(request);
			if(conn==null)
			{
			   	response.sendRedirect(request.getContextPath() + "/login.html");
			   	return;
			}
			
			String orgId = conn.getUserInfo().getOrganizationId();
			if(type.equalsIgnoreCase("xmd"))
			{
				String datasetAlias = request.getParameter("datasetAlias");
				if(datasetAlias==null || datasetAlias.trim().isEmpty())
				{
					throw new IllegalArgumentException("datasetAlias is a required param");
				}
				String datasetId = request.getParameter("datasetId");
				String datasetVersion = request.getParameter("datasetVersion");

				File dataDir = DatasetUtilConstants.getDataDir(orgId);
				File datasetDir = new File(dataDir,datasetAlias);
				FileUtils.forceMkdir(datasetDir);
				ObjectMapper mapper = new ObjectMapper();	
				@SuppressWarnings("rawtypes")
				Map xmdObject =  mapper.readValue(jsonString, Map.class);
				File outfile = new File(datasetDir,"user.xmd.json");
				mapper.writerWithDefaultPrettyPrinter().writeValue(outfile , xmdObject);				
				if(!XmdUploader.uploadXmd(outfile.getAbsolutePath(), datasetAlias, datasetId, datasetVersion, conn))
				{
					throw new IllegalArgumentException("Failed to uplaod XMD");
				}
			}else if(type.equalsIgnoreCase("dataflow"))
			{
				String dataflowAlias = request.getParameter("dataflowAlias");
				if(dataflowAlias==null || dataflowAlias.trim().isEmpty())
				{
					throw new IllegalArgumentException("dataflowAlias is a required param");
				}
				
				String dataflowId = request.getParameter("dataflowId");
				String temp = request.getParameter("create");
				boolean create = false;
				if(temp!=null && temp.trim().equalsIgnoreCase("true"))
					create = true;

				String dataflowMasterLabel = dataflowAlias;
				String devName = ExternalFileSchema.createDevName(dataflowAlias, "dataFlow", 1, false);

//				File dataDir = DatasetUtilConstants.getDataflowDir(orgId);				
//				File dataFlowFile = new File(dataDir,devName+".json");
//
				ObjectMapper mapper = new ObjectMapper();	
				@SuppressWarnings("rawtypes")
				Map dataflowObject =  mapper.readValue(jsonString, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(dataFlowFile , dataflowObject);				
				DataFlowUtil.upsertDataFlow(conn, devName, dataflowId, dataflowObject, create, dataflowMasterLabel);
//				DataFlowUtil.uploadDataFlow(conn, dataflowAlias,dataflowId,dataflowObject);
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
			ResponseStatus status = new ResponseStatus("error",t.getMessage());
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(response.getOutputStream(), status);
		}
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
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

					PartnerConnection conn = AuthFilter.getConnection(request);
					if(conn==null)
					{
					   	response.sendRedirect(request.getContextPath() + "/login.html");
					   	return;
					}					
					
					if(type.equalsIgnoreCase("xmd"))
					{
						String datasetAlias = request.getParameter("datasetAlias");
						if(datasetAlias==null || datasetAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("datasetAlias is a required param");
						}
						String datasetId = request.getParameter("datasetId");
						String datasetVersion = request.getParameter("datasetVersion");
						Map<String,String> xmd = DatasetDownloader.getXMD(datasetAlias,datasetId,datasetVersion, conn);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
						@SuppressWarnings("rawtypes")
						Map xmdObject =  mapper.readValue(xmd.get("mainXmd"), Map.class);
				    	mapper.writeValue(response.getOutputStream(), xmdObject);
					}else if(type.equalsIgnoreCase("dataflow"))
					{
						String dataflowAlias = request.getParameter("dataflowAlias");
						if(dataflowAlias==null || dataflowAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("dataflowAlias is a required param");
						}
						String dataflowId = request.getParameter("dataflowId");
						DataFlow df = DataFlowUtil.getDataFlow(conn, dataflowAlias, dataflowId);
				    	ObjectMapper mapper = new ObjectMapper();
						mapper.writeValue(response.getOutputStream(), df.getWorkflowDefinition());
					}else
					{
						response.setContentType("application/json");
						response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
						ResponseStatus status = new ResponseStatus("error","Invalid Request {"+type+"}");
						ObjectMapper mapper = new ObjectMapper();
						mapper.writeValue(response.getOutputStream(), status);
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
