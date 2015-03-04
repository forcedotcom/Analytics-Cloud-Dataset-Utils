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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.DataFlowMonitorUtil;
import com.sforce.dataset.flow.monitor.JobEntry;
import com.sforce.dataset.flow.monitor.NodeEntry;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.flow.monitor.SessionHistory;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.dataset.util.DatasetType;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.FolderType;

@MultipartConfig 
public class ListServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
//	private static final ThreadPoolExecutor executorPool = new ThreadPoolExecutor(2, 4, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2), Executors.defaultThreadFactory());
//	private static List<FileUploadRequest> files = new LinkedList<FileUploadRequest>();

	/*
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try
		{
//			String pathInfo = request.getPathInfo();
//			if(pathInfo.equalsIgnoreCase("upload"))
//			{
					files.clear();
					files.addAll(MultipartRequestHandler.uploadByApacheFileUpload(request));
					CsvUploadWorker worker = new CsvUploadWorker(files, DatasetUtilServer.partnerConnection);
				    executorPool.execute(worker);
				    int cnt=0;
				    while(cnt<10)
				    {
				    	if(worker.isDone())
				    	{
				    		if(!worker.isUploadStatus())
				    		{
				    			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "File upload failed check <a href=\"file:///"+worker.getLogFile()+"\">"+worker.getLogFile()+"</a> for details");
				    			return;
				    		}
				    		break;
				    	}
				    	try
				    	{
				    		Thread.sleep(3000);
				    	}catch(Throwable t)
				    	{
				    		break;
				    	}
				    	cnt++;
				    }
				    
				    
				    response.setContentType("application/json");
			    	ObjectMapper mapper = new ObjectMapper();
			    	mapper.writeValue(response.getOutputStream(), files);
//			}else
//			{
//				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+request.getPathInfo()+"}");
//			}
		}catch(Throwable t)
		{
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+t.toString()+"}");
		}
	}
*/
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try 
		{		
//			String pathInfo = request.getPathInfo();
//			if(pathInfo.equalsIgnoreCase("upload"))
//			{
					String value = request.getParameter("type");
					if(value==null || value.trim().isEmpty())
					{
						throw new IllegalArgumentException("type is required param");
					}
					
					if(value.equalsIgnoreCase("folder"))
					{
						List<FolderType> folders = DatasetUtils.listFolders(DatasetUtilServer.partnerConnection);
						FolderType def = new FolderType();
						def.name = DatasetUtilConstants.defaultAppName;
						def.developerName = DatasetUtilConstants.defaultAppName;
						def._type = "folder";
						folders.add(0, def);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), folders);
					}else if(value.equalsIgnoreCase("dataset"))
					{
						List<DatasetType> datasets = DatasetUtils.listDatasets(DatasetUtilServer.partnerConnection);
						DatasetType def = new DatasetType();
						def.name = "";
						def._alias = "";
						def._type = "edgemart";
						datasets.add(0, def);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), datasets);
					}else if(value.equalsIgnoreCase("session"))
					{
						List<Session> sessions = DatasetUtils.listSessions(DatasetUtilServer.partnerConnection);
						for(Session s:sessions)
						{
							if(s.getStatus().equalsIgnoreCase("COMPLETED"))
							{
								String serverStatus = s.getParam(DatasetUtilConstants.serverStatusParam);
								if(serverStatus == null || (serverStatus.equalsIgnoreCase("New") || serverStatus.equalsIgnoreCase("Queued") || serverStatus.replaceAll(" ", "").equalsIgnoreCase("InProgress")))
								{
									String hdrId = s.getParam(DatasetUtilConstants.hdrIdParam);
									if(hdrId != null && !hdrId.trim().isEmpty())
									{
										try
										{
											String temp = DatasetLoader.getUploadedFileStatus(DatasetUtilServer.partnerConnection, hdrId);
											if(temp!=null && !temp.isEmpty())
											{
												s.setParam(DatasetUtilConstants.serverStatusParam,temp.toUpperCase());
											}
										}catch(Throwable t)
										{
											t.printStackTrace();
										}
									}
								}
							}
						}
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), sessions);
					}else if(value.equalsIgnoreCase("sessionHistory"))
					{
//						LinkedList<SessionHistory> sessions = new LinkedList<SessionHistory>();
						List<JobEntry> jobs = DataFlowMonitorUtil.getDataFlowJobs(DatasetUtilServer.partnerConnection, null);
						String orgId = DatasetUtilServer.partnerConnection.getUserInfo().getOrganizationId();
						long twoDayAgo = System.currentTimeMillis() - 2*24*60*60*1000;
						for(JobEntry job:jobs)
						{
							long lastupdated = job.getStartTimeEpoch();
							if(job.getEndTimeEpoch()!=0)
							{
								lastupdated = job.getEndTimeEpoch();
							}
							if(job.getType().equalsIgnoreCase("system") && lastupdated > twoDayAgo)
							{
								SessionHistory sessionHistory = SessionHistory.getSessionByJobTrackerId(orgId, job.get_uid());
								if(sessionHistory==null)
								{
									sessionHistory = new SessionHistory(orgId, job);
								}
								
								if(job.getStatus()==2 || !sessionHistory.isNodeDetailsFetched())
								{									
									List<NodeEntry> nodes = DataFlowMonitorUtil.getDataFlowJobNodes(DatasetUtilServer.partnerConnection, job.getNodeUrl());
									for(NodeEntry node:nodes)
									{
										if(node.getNodeType() != null && (node.getNodeType().equalsIgnoreCase("csvDigest") || node.getNodeType().equalsIgnoreCase("binDigest")))
										{
											sessionHistory.setTargetTotalRowCount(node.getOutputRowsProcessed());
											sessionHistory.setTargetErrorCount(node.getOutputRowsFailed());
											if(job.getEndTimeEpoch()!=0L)
												sessionHistory.updateLastModifiedTime(job.getEndTimeEpoch());
											if(job.getStatus()!=2)
											{
												sessionHistory.setNodeDetailsFetched(true);
											}
										}
									}
								}
//								sessions.add(sessionHistory);
							}
						}
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), SessionHistory.listSessions(orgId));
					}else
					{
						response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+value+"}");
					}
					
//					FileUploadRequest getFile = files.get(Integer.parseInt(value));
//					response.setContentType(getFile.getInputFileType());
//				 	
//				 	response.setHeader("Content-disposition", "attachment; filename=\""+getFile.getInputFileName()+"\"");
//				 	
//			        InputStream input = new FileInputStream(getFile.savedFile);
//			        OutputStream output = response.getOutputStream();
//			        IOUtils.copy(input, output);
//			        output.close();
//			        input.close();
					
//			}else
//			{
//				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+request.getPathInfo()+"}");
//			}
		 }catch (Throwable t) {
			 	t.printStackTrace();
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+t.toString()+"}");
		 }
	}
}
