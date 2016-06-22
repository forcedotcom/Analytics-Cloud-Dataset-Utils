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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.metadata.DatasetXmd;
import com.sforce.dataset.saql.SaqlUtil;
import com.sforce.dataset.server.auth.AuthFilter;
import com.sforce.dataset.server.preview.Header;
import com.sforce.dataset.server.preview.PreviewData;
import com.sforce.dataset.server.preview.PreviewUtil;
import com.sforce.dataset.util.DatasetDownloader;
import com.sforce.soap.partner.PartnerConnection;

@MultipartConfig 
public class PreviewServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		doGet(request, response);
		/*
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
				File dataDir = DatasetUtilConstants.getDataDir(orgId);
				File datasetDir = new File(dataDir,datasetAlias);
				FileUtils.forceMkdir(datasetDir);
				ObjectMapper mapper = new ObjectMapper();	
				@SuppressWarnings("rawtypes")
				Map xmdObject =  mapper.readValue(jsonString, Map.class);
				File outfile = new File(datasetDir,"user.xmd.json");
				mapper.writerWithDefaultPrettyPrinter().writeValue(outfile , xmdObject);				
				XmdUploader.uploadXmd(outfile.getAbsolutePath(), datasetAlias, conn);
			}else if(type.equalsIgnoreCase("dataflow"))
			{
				String dataflowName = request.getParameter("dataflowName");
				if(dataflowName==null || dataflowName.trim().isEmpty())
				{
					throw new IllegalArgumentException("dataflowName is a required param");
				}
				DataFlow df = dfMap.get(dataflowName);
				if(df != null)
				{
					ObjectMapper mapper = new ObjectMapper();	
					df.workflowDefinition = mapper.readValue(jsonString, Map.class);
					DataFlowUtil.uploadDataFlow(conn, df);
				}else
				{
					throw new IllegalArgumentException("dataflowName {"+dataflowName+"} not found");
				}
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
		*/
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");
		String saql = null;
		try 
		{		
					String type = request.getParameter("type");
					if(type==null || type.trim().isEmpty())
					{
						throw new IllegalArgumentException("type is a required param");
					}

					String name = request.getParameter("name");
					if(name==null || name.trim().isEmpty())
					{
						throw new IllegalArgumentException("name is a required param");
					}

					PartnerConnection conn = AuthFilter.getConnection(request);
					if(conn==null)
					{
					   	response.sendRedirect(request.getContextPath() + "/login.html");
					   	return;
					}					
					
					if(type.equalsIgnoreCase("file"))
					{

						
						String orgId = conn.getUserInfo().getOrganizationId();
						File dataDir = DatasetUtilConstants.getDataDir(orgId);

						File inputFile = new File(dataDir,name);
						if(inputFile==null || !inputFile.exists() || inputFile.length()==0)
						{
							throw new IllegalArgumentException("file {"+name+"} not found");
						}
						
						List<Header> columns = PreviewUtil.getFileHeader(inputFile,orgId);
						List<Map<String,Object>> data = PreviewUtil.getFileData(inputFile, orgId);
						PreviewData resp = new PreviewData();

						resp.setColumns(columns);
						resp.setData(data);
						
						response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), resp);
					}else if(type.equalsIgnoreCase("dataset"))
					{
						Map<String, String> xmd = DatasetDownloader.getXMD(name, conn);
						if(xmd==null || xmd.isEmpty())
						{
							throw new IllegalArgumentException("Internal server error fetching dataset {"+name+"}");
						}

						saql = request.getParameter("saql");
						if(saql==null)
						{
							ObjectMapper mapper = new ObjectMapper();	
							mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
							DatasetXmd xmdValue = mapper.readValue(xmd.get("mainXmd"),DatasetXmd.class);
							saql = SaqlUtil.generateSaql(conn, xmd.get("datasetId"), xmd.get("datasetVersion"), xmdValue);
						}

						List<Map<String,Object>> data = SaqlUtil.queryDataset(conn, saql);
						if(data==null || data.isEmpty())
						{
							throw new IllegalArgumentException("could not query dataset {"+name+"}");
						}
						
						PreviewData resp = new PreviewData();

						List<Header> hdr = PreviewUtil.getSaqlHeader(data);
						resp.setColumns(hdr);
						resp.setData(PreviewUtil.getSaqlData(data,hdr));
						resp.setSaql(saql);
						response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), resp);
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
				if(saql!=null)
				{
					Map<String,String> statusData = new LinkedHashMap<String,String>();
					statusData.put("saql", saql);
					status.setStatusData(statusData);
				}
				ObjectMapper mapper = new ObjectMapper();
				mapper.writeValue(response.getOutputStream(), status);
		 }
	}
}
