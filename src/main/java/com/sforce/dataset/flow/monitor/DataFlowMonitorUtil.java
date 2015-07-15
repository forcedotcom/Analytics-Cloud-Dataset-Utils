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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.util.HttpUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class DataFlowMonitorUtil {
	
	
	public static void getJobsAndErrorFiles(PartnerConnection partnerConnection, String datasetName) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		List<JobEntry> jobs = getDataFlowJobs(partnerConnection, datasetName, null);
		if(jobs!=null)
		{
			int cnt = 0;
			for(JobEntry job:jobs)
			{
				if(cnt>4)
					break;
				getJobErrorFile(partnerConnection, datasetName, job._uid);
				cnt++;
			}
		}
	}

	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<JobEntry> getDataFlowJobs(PartnerConnection partnerConnection, String datasetName, String dataflowId) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		List<JobEntry> jobsList = new LinkedList<JobEntry>(); 
//		System.out.println();
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();

		URI u = new URI(serviceEndPoint);

		String url = "/insights/internal_api/v1.0/esObject/jobs";
		if(dataflowId!=null)
		{
			 url = String.format("/insights/internal_api/v1.0/esObject/workflow/%s/jobs",dataflowId);
		}
		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), url, null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
//		System.out.println("Fetching job list from server, this may take a minute...");
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
	       if (statusCode != HttpStatus.SC_OK) {
		       throw new IOException(String.format("getDataFlowJobs failed: %d %s", statusCode,reasonPhrase));
	       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();
		
		if(emList!=null && !emList.isEmpty())
		{
				ObjectMapper mapper = new ObjectMapper();	
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				Map res =  mapper.readValue(emList, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
				List<Map> jobs = (List<Map>) res.get("result");
				if(jobs != null && !jobs.isEmpty())
				{
					for(Map job:jobs)
					{
						String _type = (String) job.get("_type");
						if(_type != null && _type.equals("jobs"))
						{
							String workflowName = (String) job.get("workflowName");
							if(workflowName != null && (datasetName == null || datasetName.isEmpty() || workflowName.startsWith(datasetName)))
							{
								JobEntry jobEntry = new JobEntry();
								
								Object temp = job.get("_createdDateTime");
								if(temp != null && temp instanceof Number)
								{
									jobEntry._createdDateTime = ((Number)temp).longValue();
								}else
								{
									System.out.println("invalid _createdDateTime value {"+temp+"}");
								}
								
								jobEntry._type  = (String) job.get("_type");
								
								jobEntry._uid  = (String) job.get("_uid");
								
								temp = job.get("duration");
								if(temp != null && temp instanceof Number)
								{
									jobEntry.duration = ((Number)temp).longValue();
								}else
								{
									System.out.println("invalid duration value {"+temp+"}");
								}
								
								jobEntry.endTime = (String) job.get("endTime");
								
								temp = job.get("endTimeEpoch");
								if(temp != null && temp instanceof Number)
								{
									jobEntry.endTimeEpoch = ((Number)temp).longValue();
								}else
								{
									System.out.println("invalid endTimeEpoch value {"+temp+"}");
								}
								
								jobEntry.errorMessage = (String) job.get("errorMessage");
								
								jobEntry.nodeUrl = (String) job.get("nodeUrl");
								
								jobEntry.startTime = (String) job.get("startTime");
								
								temp = job.get("startTimeEpoch");
								if(temp != null && temp instanceof Number)
								{
									jobEntry.startTimeEpoch = ((Number)temp).longValue();
								}else
								{
									System.out.println("invalid startTimeEpoch value {"+temp+"}");
								}
								
								temp = job.get("status");
								if(temp != null && temp instanceof Number)
								{
									jobEntry.status = ((Number)temp).intValue();
								}else
								{
									System.out.println("invalid status value {"+temp+"}");
								}
								
								jobEntry.type = (String) job.get("type");
								
								jobEntry.workflowName = (String) job.get("workflowName");
								
								jobsList.add(jobEntry);
							}
						}else
						{
					       throw new IOException(String.format("Dataflow job list download failed, invalid server response %s",emList));
						}
					} //end for
				}else
				{
			       throw new IOException(String.format("Dataflow job list download failed, invalid server response %s",emList));
				}
		}
//		System.out.println("Found {"+jobsList.size()+"} jobs for dataset {"+datasetName+"}");
		Collections.sort(jobsList, Collections.reverseOrder());
		return jobsList;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<NodeEntry> getDataFlowJobNodes(PartnerConnection partnerConnection, String nodeUrl) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		List<NodeEntry> nodeList = new LinkedList<NodeEntry>(); 
//		System.out.println();
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		URI u = new URI(serviceEndPoint);

		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), nodeUrl, null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
//		System.out.println("Fetching node list from server, this may take a minute...");
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
	       if (statusCode != HttpStatus.SC_OK) {
		       throw new IOException(String.format("getDataFlowJobs failed: %d %s", statusCode,reasonPhrase));
	       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();
		
		if(emList!=null && !emList.isEmpty())
		{
				ObjectMapper mapper = new ObjectMapper();	
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				Map res =  mapper.readValue(emList, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
				List<Map> jobs = (List<Map>) res.get("result");
				if(jobs != null && !jobs.isEmpty())
				{
					for(Map job:jobs)
					{
						String _type = (String) job.get("_type");
						if(_type != null && _type.equals("nodes"))
						{
							NodeEntry nodeEntry = new NodeEntry();
								
								
								nodeEntry.startTime  = (String) job.get("startTime");
								
								nodeEntry._uid  = (String) job.get("_uid");
								nodeEntry.duration = (String) job.get("duration");
								
								nodeEntry.nodeName = (String) job.get("nodeName");
								
								nodeEntry.nodeType = (String) job.get("nodeType");
								
								nodeEntry._type = _type;
								
								nodeEntry.status = (String) job.get("status");
								
								Object temp = job.get("outputRowsFailed");
								if(temp != null)
								{
									if(temp instanceof Number)
									{
										nodeEntry.outputRowsFailed = ((Number)temp).longValue();
									}else
									{
										BigDecimal bd = new BigDecimal(temp.toString());
										nodeEntry.outputRowsFailed = bd.longValue();
									}
								}
								
								temp = job.get("outputRowsProcessed");
								if(temp != null)
								{
									if(temp != null && temp instanceof Number)
									{
										nodeEntry.outputRowsProcessed = ((Number)temp).longValue();
									}else
									{
										BigDecimal bd = new BigDecimal(temp.toString());
										nodeEntry.outputRowsProcessed = bd.longValue();
									}
								}								
								nodeList.add(nodeEntry);
							}
					}
				}else
				{
			       throw new IOException(String.format("Dataflow job list download failed, invalid server response %s",emList));
				}
			}else
			{
		       throw new IOException(String.format("Dataflow job list download failed, invalid server response %s",emList));
			}
//		System.out.println("Found {"+nodeList.size()+"} nodes for dataset {"+nodeUrl+"}");
		return nodeList;
	}
	
	public static File getJobErrorFile(PartnerConnection partnerConnection, String datasetName, String jobTrackerid) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		if(jobTrackerid == null || jobTrackerid.trim().isEmpty())
		{
			throw new IOException("Job TrackerId cannot be null");
		}

		if(datasetName == null || datasetName.trim().isEmpty())
		{
			throw new IOException("datasetName cannot be null");
		}

//		System.out.println();
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();
		String orgId = partnerConnection.getUserInfo().getOrganizationId();

		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		URI u = new URI(serviceEndPoint);
		
		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/jobTrackerHeartbeat/{0}/nodes/digest/nodeerrorlog".replace("{0}", jobTrackerid), null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
//		System.out.println("Fetching error sessionLog for job {"+jobTrackerid+"} from server...");
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
	       if (statusCode != HttpStatus.SC_OK) {
		       throw new IOException(String.format("getDataFlowJob node error sessionLog failed: %d %s", statusCode,reasonPhrase));
	       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();		
		
		File dataDir = DatasetUtilConstants.getDataDir(orgId);
		File parent = new File(dataDir,datasetName);
		FileUtils.forceMkdir(parent);

		File outfile = new File(parent, datasetName+"_"+jobTrackerid+"_error.csv");
		System.out.println("fetching file {"+outfile+"}. Content-length {"+emresponseEntity.getContentLength()+"}");
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outfile),DatasetUtilConstants.DEFAULT_BUFFER_SIZE);
		IOUtils.copy(emis, out);
		out.close();
		emis.close();
		httpClient.close();
//		System.out.println("file {"+outfile+"} downloaded. Size{"+outfile.length()+"}\n");
		return outfile;
		
//		if(emList!=null && !emList.isEmpty())
//		{
//				ObjectMapper mapper = new ObjectMapper();	
//				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//				Map res =  mapper.readValue(emList, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
//				List<Map> jobs = (List<Map>) res.get("result");
//				if(jobs != null && !jobs.isEmpty())
//				{
//					for(Map job:jobs)
//					{
//						String _type = (String) job.get("_type");
//						if(_type != null && _type.equals("jobs"))
//						{
//							String workflowName = (String) job.get("workflowName");
//							if(workflowName != null && (datasetName == null || datasetName.isEmpty() || workflowName.startsWith(datasetName)))
//							{
//								FolderType jobEntry = new FolderType();
//								jobEntry._createdDateTime = (Integer) job.get("_createdDateTime");
//								jobEntry._type  = (String) job.get("_type");
//								jobEntry._uid  = (String) job.get("_uid");
//								jobEntry.duration = (Integer) job.get("duration");
//								jobEntry.endTime = (String) job.get("endTime");
//								jobEntry.endTimeEpoch = (Long) job.get("endTimeEpoch");
//								jobEntry.errorMessage = (String) job.get("errorMessage");
//								jobEntry.nodeUrl = (String) job.get("nodeUrl");
//								jobEntry.startTime = (String) job.get("startTime");
//								jobEntry.startTimeEpoch = (Long) job.get("startTimeEpoch");
//								jobEntry.status = (Integer) job.get("status");
//								jobEntry.type = (String) job.get("type");
//								jobEntry.workflowName = (String) job.get("workflowName");
//								jobsList.add(jobEntry);
//							}
//						}else
//						{
//					       throw new IOException(String.format("Dataflow job list download failed, invalid server response %s",emList));
//						}
//					} //end for
//				}else
//				{
//			       throw new IOException(String.format("Dataflow job list download failed, invalid server response %s",emList));
//				}
//		}


	}


}
