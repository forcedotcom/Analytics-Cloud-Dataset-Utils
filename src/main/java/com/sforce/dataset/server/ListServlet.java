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
import java.util.LinkedHashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.DataFlow;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.flow.monitor.DataFlowMonitorUtil;
import com.sforce.dataset.flow.monitor.JobEntry;
import com.sforce.dataset.flow.monitor.NodeEntry;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.flow.monitor.SessionHistory;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.scheduler.Schedule;
import com.sforce.dataset.scheduler.SchedulerUtil;
import com.sforce.dataset.server.auth.AuthFilter;
import com.sforce.dataset.util.DatasetType;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.FolderType;
import com.sforce.soap.partner.PartnerConnection;

public class ListServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");

		try 
		{		
					String value = request.getParameter("type");
					if(value==null || value.trim().isEmpty())
					{
						throw new IllegalArgumentException("type is required param");
					}
					
					PartnerConnection conn = AuthFilter.getConnection(request);
					if(conn==null)
					{
					   	response.sendRedirect(request.getContextPath() + "/login.html");
					   	return;
					}

					if(value.equalsIgnoreCase("dataflow"))
					{
						List<DataFlow> flowList = DataFlowUtil.listDataFlow(conn);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), flowList);
					}
					else if(value.equalsIgnoreCase("schedule"))
					{
						String scheduleAlias = request.getParameter("scheduleAlias");
						if(scheduleAlias!=null && !scheduleAlias.trim().isEmpty())
						{
							Schedule sched = SchedulerUtil.readSchedule(conn, scheduleAlias);
						    response.setContentType("application/json");
					    	ObjectMapper mapper = new ObjectMapper();
					    	mapper.writeValue(response.getOutputStream(), sched);
						}else
						{
							List<Schedule> list = SchedulerUtil.listSchedules(conn);
						    response.setContentType("application/json");
					    	ObjectMapper mapper = new ObjectMapper();
					    	mapper.writeValue(response.getOutputStream(), list);
						}
					}
					else if(value.equalsIgnoreCase("scheduleDelete"))
					{
						String scheduleAlias = request.getParameter("scheduleAlias");
						if(scheduleAlias==null || scheduleAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("scheduleAlias is required param");
						}
						SchedulerUtil.deleteSchedule(conn, scheduleAlias);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), scheduleAlias);
					}
					else if(value.equalsIgnoreCase("scheduleEnable"))
					{
						String scheduleAlias = request.getParameter("scheduleAlias");
						if(scheduleAlias==null || scheduleAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("scheduleAlias is required param");
						}
						SchedulerUtil.enableSchedule(conn, scheduleAlias);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), scheduleAlias);
					}
					else if(value.equalsIgnoreCase("scheduleDisable"))
					{
						String scheduleAlias = request.getParameter("scheduleAlias");
						if(scheduleAlias==null || scheduleAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("scheduleAlias is required param");
						}
						SchedulerUtil.disableSchedule(conn, scheduleAlias);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), scheduleAlias);
					}
					else if(value.equalsIgnoreCase("dataflowDelete"))
					{
						String dataflowAlias = request.getParameter("dataflowAlias");
						if(dataflowAlias==null || dataflowAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("dataflowAlias is required param");
						}
						DataFlowUtil.deleteDataFlow(conn, dataflowAlias);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), dataflowAlias);
					}
					else if(value.equalsIgnoreCase("dataflowStart"))
					{
						String dataflowAlias = request.getParameter("dataflowAlias");
						if(dataflowAlias==null || dataflowAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("dataflowAlias is required param");
						}
						LinkedHashMap<String,String> jobDataMap = new LinkedHashMap<String,String>();
						jobDataMap.put(dataflowAlias, "DataFlow");
						SchedulerUtil.startNow(conn,dataflowAlias+"startNow", jobDataMap);
						
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), dataflowAlias);
					}
					else if(value.equalsIgnoreCase("dataflowCopy"))
					{
						String dataflowAlias = request.getParameter("dataflowAlias");
						if(dataflowAlias==null || dataflowAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("dataflowAlias is required param");
						}
						String dataflowId = request.getParameter("dataflowId");						
						DataFlowUtil.copyDataFlow(conn, dataflowAlias, dataflowId);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), dataflowAlias);
					} else if(value.equalsIgnoreCase("folder"))
					{
						List<FolderType> folders = DatasetUtils.listFolders(conn);
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
						boolean current = false;
						String tmp = request.getParameter("current");
						if(tmp!=null && tmp.trim().equalsIgnoreCase("true"))
						{
							current = true;
						}
						List<DatasetType> datasets = DatasetUtils.listDatasets(conn, current);
						DatasetType def = new DatasetType();
						def.name = "";
						def._alias = "";
						def._type = "edgemart";
						datasets.add(0, def);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), datasets);
					}else if(value.equalsIgnoreCase("datasetAndApps"))
					{
						boolean current = false;
						String tmp = request.getParameter("current");
						if(tmp!=null && tmp.trim().equalsIgnoreCase("true"))
						{
							current = true;
						}
						List<DatasetType> datasets = DatasetUtils.listDatasets(conn, current);
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), datasets);
					}else if(value.equalsIgnoreCase("deleteDataset"))
					{
						String datasetAlias = request.getParameter("datasetAlias");
						if(datasetAlias==null || datasetAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("datasetAlias is required param");
						}
						String datasetId = request.getParameter("datasetId");
						if(!DatasetUtils.deleteDataset(datasetAlias, datasetId, conn))
						{
							throw new IllegalArgumentException("Dataset {"+datasetAlias+"} could not be deleted");
						}
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), datasetAlias);
					}else if(value.equalsIgnoreCase("session"))
					{
						List<Session> sessions = DatasetUtils.listSessions(conn);
						for(Session s:sessions)
						{
							if(s.getType().equalsIgnoreCase("FileUpload"))
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
												String temp = DatasetLoader.getUploadedFileStatus(conn, hdrId);
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
						}
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), sessions);
					}else if(value.equalsIgnoreCase("sessionHistory"))
					{
						List<JobEntry> jobs = DataFlowMonitorUtil.getDataFlowJobs(conn, null, null);
						String orgId = conn.getUserInfo().getOrganizationId();
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
									List<NodeEntry> nodes = DataFlowMonitorUtil.getDataFlowJobNodes(conn, job.getNodeUrl());
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
							}
						}
					    response.setContentType("application/json");
				    	ObjectMapper mapper = new ObjectMapper();
				    	mapper.writeValue(response.getOutputStream(), SessionHistory.listSessions(orgId));
				
					}else if(value.equalsIgnoreCase("metadataJson"))
					{
						String datasetAlias = request.getParameter("datasetAlias");
						if(datasetAlias==null || datasetAlias.trim().isEmpty())
						{
							throw new IllegalArgumentException("datasetAlias is required param");
						}
						
						ExternalFileSchema schema = DatasetLoader.getLastUploadedJson(conn, datasetAlias, System.out);
				    	ObjectMapper mapper = new ObjectMapper();
						if(schema!=null)
						{
							response.setContentType("application/json");
							response.setHeader("Content-disposition", "attachment; filename=\""+datasetAlias+"_schema.json"+"\"");
					        OutputStream output = response.getOutputStream();
					    	mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), schema);
					        output.close();
						}else
						{
							response.setContentType("application/json");
							response.setHeader("Content-disposition", "attachment; filename=\""+datasetAlias+"_schema.json"+"\"");
							ResponseStatus status = new ResponseStatus("error","Matadata json for Dataset {"+datasetAlias+"} not found");
					        OutputStream output = response.getOutputStream();
					    	mapper.writerWithDefaultPrettyPrinter().writeValue(response.getOutputStream(), status);
					        output.close();
						}
					}else
					{
						response.setContentType("application/json");
						response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
						ResponseStatus status = new ResponseStatus("error","Invalid Request {"+value+"}");
						ObjectMapper mapper = new ObjectMapper();
						mapper.writeValue(response.getOutputStream(), status);
					}

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
			
			PartnerConnection conn = AuthFilter.getConnection(request);
			if(conn==null)
			{
			   	response.sendRedirect(request.getContextPath() + "/login.html");
			   	return;
			}
			
			if(type.equalsIgnoreCase("schedule"))
			{
				String scheduleAlias = request.getParameter("scheduleAlias");
				if(scheduleAlias==null || scheduleAlias.trim().isEmpty())
				{
					throw new IllegalArgumentException("scheduleAlias is a required param");
				}

				String temp = request.getParameter("startDateTime");
				if(temp==null || temp.trim().isEmpty())
				{
					throw new IllegalArgumentException("startDateTime is a required param");
				}
				long startDateTime = (new BigDecimal(temp.trim())).longValue();

				String frequency = request.getParameter("frequency");
				if(frequency==null || frequency.trim().isEmpty())
				{
					throw new IllegalArgumentException("frequency is a required param");
				}
				
				temp = request.getParameter("interval");
				if(temp==null || temp.trim().isEmpty())
				{
					throw new IllegalArgumentException("interval is a required param");
				}
				
				long interval = (new BigDecimal(temp.trim())).longValue();
				
				String jobs[] = request.getParameterValues("jobs[]");
				if(jobs==null || jobs.length==0)
				{
					throw new IllegalArgumentException("jobs is a required param");
				}

				temp = request.getParameter("create");
				boolean isCreate = false;
				if(temp!=null && !temp.trim().isEmpty() && temp.equalsIgnoreCase("true"))
				{
					isCreate = true;
				}

				
				LinkedHashMap<String,String> jobDataMap = new LinkedHashMap<String,String>();
				for(String job:jobs)
				{
					jobDataMap.put(job, "DataFlow");
				}
				
				SchedulerUtil.saveSchedule(conn, scheduleAlias, null, frequency, interval, jobDataMap, startDateTime, isCreate);
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
	
}
