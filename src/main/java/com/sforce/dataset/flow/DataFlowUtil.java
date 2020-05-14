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
package com.sforce.dataset.flow;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.FileUtilsExt;
import com.sforce.dataset.util.HttpUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class DataFlowUtil {
	
	//private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("EEEE MMMM d HH:mm:ss z yyyy"); //Mon Jun 15 00:12:03 GMT 2015
	private static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX"); //Mon Jun 15 00:12:03 GMT 2015
	private  static final String dataflowURL = "/insights/internal_api/v1.0/esObject/workflow/%s/json";
	//New public endpoint . Only required in start as other methods deprecated
	private static final String dataflowRunURL = "/services/data/v48.0/wave/dataflowjobs";
	
	@SuppressWarnings("rawtypes")
	public static void uploadAndStartDataFlow(PartnerConnection partnerConnection, Map wfdef, String workflowName) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		DataFlow df = getDataFlow(partnerConnection, workflowName, null);
		if(df!=null)
		{
			uploadDataFlow(partnerConnection, df.getName(), df.get_uid(), wfdef);
			startDataFlow(partnerConnection, df.getName(), df.get_uid());
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void uploadDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId, Map dataflowObject) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		if(dataflowId==null|| dataflowId.trim().isEmpty())
		{
        	throw new IllegalArgumentException("dataflowId is required param");
		}
		
		if(dataflowObject==null)
		{
        	throw new IllegalArgumentException("dataflowObject is required param");
		}
		
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();

		URI u = new URI(serviceEndPoint);

		URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), String.format(dataflowURL, dataflowId), null,null);			
		
        HttpPatch httpPatch = new HttpPatch(patchURI);
        
		Map map = new LinkedHashMap();
		map.put("workflowDefinition", dataflowObject);
		ObjectMapper mapper = new ObjectMapper();			
        StringEntity entity = new StringEntity(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map), "UTF-8");        
        entity.setContentType("application/json");
        httpPatch.setConfig(requestConfig);
        httpPatch.setEntity(entity);
        httpPatch.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(httpPatch);
	   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
       int statusCode = emresponse.getStatusLine().getStatusCode();
//       if (statusCode != HttpStatus.SC_OK) {
//	       throw new IOException(String.format("Dataflow {%s} upload failed: %d %s", dataflowAlias,statusCode,reasonPhrase));
//       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
//		System.out.println(emList);
		emis.close();
		httpClient.close();

		if (statusCode != HttpStatus.SC_CREATED) 
	    {
			String errorCode = statusCode+"";
	    	try
	    	{
	   		ObjectMapper mapper1 = new ObjectMapper();	
			mapper1.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			Map res =  mapper1.readValue(emList, Map.class);
			String temp = (String) res.get("errorMsg");
			String temp1 = (String) res.get("errorCode");
			
			if(temp != null && !temp.trim().isEmpty())
			{
				reasonPhrase = temp;
				errorCode = temp1;
			}
	    	}catch(Exception e)
	    	{
	    		e.printStackTrace();
	    		System.out.println("Server response:"+emList);
	    	}
		       throw new IOException(String.format("Dataflow %s upload failed: %s - %s", dataflowAlias,errorCode,reasonPhrase));
	       }

		
		System.out.println("Dataflow {"+dataflowAlias+"} successfully uploaded");

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static DataFlow getDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		DataFlow df = null;
//		partnerConnection.getServerTimestamp();
		
//		if(dataflowId==null)
//		{
			List<DataFlow> dfList = listDataFlow(partnerConnection);
			for(DataFlow _df:dfList)
			{
				if(_df.getName().equals(dataflowAlias))
				{
					dataflowId = _df.get_uid();
					df = _df;
				}
			}
//		}

		if(df == null)
		{
			throw new IllegalArgumentException("dataflowAlias {"+dataflowAlias+"} not found");
		}
		
		if(df.getWorkflowType().equalsIgnoreCase("local"))
		{
			return getDataFlowLocal(partnerConnection, dataflowAlias);
		}
		
//		if(df == null)
//		{
//			df = new DataFlow();
//			df.name = dataflowAlias;
//			df._uid = dataflowId;
//			df.WorkflowType = "User";
//		}
		
//		String orgId = partnerConnection.getUserInfo().getOrganizationId();

		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		   
		URI u = new URI(serviceEndPoint);
		
//		File dataDir = DatasetUtilConstants.getDataDir(orgId);
//		
//		File dataFlowFile = new File(dataDir,dataflowAlias+".json");

		URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), String.format(dataflowURL, dataflowId), null,null);			
		HttpGet listEMPost1 = new HttpGet(listEMURI1);
	
		listEMPost1.setConfig(requestConfig);
		listEMPost1.addHeader("Authorization","OAuth "+sessionID);			
	
		CloseableHttpResponse emresponse1 = httpClient.execute(listEMPost1);
	
		String reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
		int statusCode = emresponse1.getStatusLine().getStatusCode();
//		if (statusCode != HttpStatus.SC_OK) {
//			throw new IOException(String.format("Dataflow %s download failed: %d %s", dataflowAlias,statusCode,reasonPhrase));
//		}
	
		HttpEntity emresponseEntity1 = emresponse1.getEntity();
		InputStream emis1 = emresponseEntity1.getContent();
		String dataFlowJson = IOUtils.toString(emis1, "UTF-8");								
		emis1.close();
		httpClient.close();

		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
		
		if (statusCode != HttpStatus.SC_OK) 
	    {
			String errorCode = statusCode+"";
	    	try
	    	{
	   		ObjectMapper mapper1 = new ObjectMapper();	
			mapper1.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			Map res =  mapper1.readValue(dataFlowJson, Map.class);
			String temp = (String) res.get("errorMsg");
			String temp1 = (String) res.get("errorCode");
			
			if(temp != null && !temp.trim().isEmpty())
			{
				reasonPhrase = temp;
				errorCode = temp1;
			}
	    	}catch(Exception e)
	    	{
	    		e.printStackTrace();
	    		System.out.println("Server response:"+dataFlowJson);
	    	}
		       throw new IOException(String.format("Dataflow %s download failed: %s - %s", dataflowAlias,errorCode,reasonPhrase));
	       }

		

		if(dataFlowJson!=null && !dataFlowJson.isEmpty())
		{
				Map res2 =  mapper.readValue(dataFlowJson, Map.class);
				List<Map> flows2 = (List<Map>) res2.get("result");
				if(flows2 != null && !flows2.isEmpty())
				{
					Map flow2 = flows2.get(0);
					if(flow2!=null)
					{
						Map wfdef = (Map) flow2.get("workflowDefinition");
						df.setWorkflowDefinition(wfdef);
//						mapper.writerWithDefaultPrettyPrinter().writeValue(dataFlowFile, df);
//						System.out.println("file {"+dataFlowFile+"} downloaded. Size{"+dataFlowFile.length()+"}");
					}else
					{
					       throw new IOException(String.format("Dataflow download failed, invalid server response %s",dataFlowJson));
					}
				}else
				{
				       throw new IOException(String.format("Dataflow download failed, invalid server response %s",dataFlowJson));
				}
		}else
		{
		       throw new IOException(String.format("Dataflow download failed, invalid server response %s",dataFlowJson));
		}
		if(df!=null)
		{
			saveDataFlow(partnerConnection, df);
		}		
		return df;
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<DataFlow> listDataFlow(PartnerConnection partnerConnection) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		List<DataFlow> dfList = new LinkedList<DataFlow>();
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();

		URI u = new URI(serviceEndPoint);

		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/workflow", null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
//	       if (statusCode != HttpStatus.SC_OK) {
//		       throw new IOException(String.format("List Dataflow failed: %d %s", statusCode,reasonPhrase));
//	       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();
		
	       if (statusCode != HttpStatus.SC_OK) 
	       {
			String errorCode = statusCode+"";
	    	try
	    	{
	   		ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			Map res =  mapper.readValue(emList, Map.class);
			String temp = (String) res.get("errorMsg");
			String temp1 = (String) res.get("errorCode");
			
			if(temp != null && !temp.trim().isEmpty())
			{
				reasonPhrase = temp;
				errorCode = temp1;
			}
	    	}catch(Exception e)
	    	{
	    		e.printStackTrace();
	    		System.out.println("Server response:"+emList);
	    	}
		       throw new IOException(String.format("List Dataflow  failed: %s - %s",errorCode,reasonPhrase));
	       }

		
		if(emList!=null && !emList.isEmpty())
		{
				ObjectMapper mapper = new ObjectMapper();	
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				Map res =  mapper.readValue(emList, Map.class);
				List<Map> flows = (List<Map>) res.get("result");
				if(flows != null && !flows.isEmpty())
				{
					for(Map flow:flows)
					{
						String workflowType = (String) flow.get("DataflowType");
						String name = (String) flow.get("name");
						if(name != null && workflowType != null)
						{
							DataFlow df = new DataFlow();
							String workflowStatus = null;
							df.setName(name);
							df.set_uid((String) flow.get("_uid"));
							df.set_url((String) flow.get("_url"));
							df.set_type((String) flow.get("_type"));

							Object temp =  (String) flow.get("WorkflowStatus");
							if(temp != null && temp instanceof String)
							{
								workflowStatus = temp.toString();
								df.setStatus(workflowStatus);
							}
							
							
							if(workflowStatus != null && workflowStatus.equalsIgnoreCase("active"))
							{
								temp = flow.get("RefreshFrequencySec");
								if(temp != null && temp instanceof Number)
								{
									df.setRefreshFrequencySec(((Number)temp).intValue());
								}
								
								df.setNextRun((String) flow.get("nextRun"));
								if(df.getNextRun() != null)
								{
									try {
//										System.out.println(defaultDateFormat.toPattern() + " : " + df.nextRun);
										df.setNextRunTime(defaultDateFormat.parse(df.getNextRun()).getTime());
									} catch (ParseException e) {
										e.printStackTrace();
									}
								}
							}
							
							df.setMasterLabel((String) flow.get("MasterLabel"));
							df.setWorkflowType(workflowType); 

							Map<String,String> temp1 = (Map<String, String>) flow.get("_lastModifiedBy");
							df.set_lastModifiedBy(DataFlow.getUserType(df, temp1));
							dfList.add(df);						
						}else
						{
					       throw new IOException(String.format("List Dataflow failed, invalid server response %s",emList));
						}
					} //end for
				}else
				{
			       throw new IOException(String.format("List Dataflow failed, invalid server response %s",emList));
				}
		}
		List<DataFlow> dfList2 = listDataFlowLocal(partnerConnection);
		if(dfList2.size()>0)
		{
			for(DataFlow df:dfList)
			{
				if(df.getName()!= null && df.getName().equalsIgnoreCase("SalesEdgeEltWorkflow"))
				{
					df.setMasterLabel(df.getMasterLabel() + " (Defunct: Do not use)");
					df.setStatus("Defunct");
					df.setNextRunTime(0);
					df.setRefreshFrequencySec(0);
				}
			}
			dfList.addAll(dfList2);
		}
		return dfList;
	}

	public static boolean startDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		if(dataflowId==null|| dataflowId.trim().isEmpty())
		{
        	throw new IllegalArgumentException("dataflowId is required param");
		}

//		System.out.println();
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		URI u = new URI(serviceEndPoint);

		//URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), String.format(dataflowURL, dataflowId).replace("json", "start"), null,null);		
		
		URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), dataflowRunURL, null,null);
  
		HttpPost httpPost = new HttpPost(patchURI);
        StringEntity params =new StringEntity("{\"command\":\"start\",\"dataflowId\":\""+dataflowId+"\"}");
        System.out.println("DataflowId " +dataflowId);
        httpPost.setConfig(requestConfig);
        httpPost.addHeader("content-type", "application/json");
        httpPost.addHeader("Authorization","OAuth "+sessionID);
        httpPost.setEntity(params);		
		CloseableHttpResponse emresponse = httpClient.execute(httpPost);
	   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
       int statusCode = emresponse.getStatusLine().getStatusCode();
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();

		if (statusCode != HttpStatus.SC_CREATED ) 
	       {
			String errorCode = statusCode+"";
	    	try
	    	{
	   		ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			@SuppressWarnings("rawtypes")
			Map res =  mapper.readValue(emList, Map.class);
			String temp = (String) res.get("errorMsg");
			String temp1 = (String) res.get("errorCode");
			
			if(temp != null && !temp.trim().isEmpty())
			{
				reasonPhrase = temp;
				errorCode = temp1;
			}
	    	}catch(Exception e)
	    	{
	    		e.printStackTrace();
	    		System.out.println("Server response:"+emList);
	    	}
		       throw new IOException(String.format("Dataflow %s start failed: %s - %s", dataflowAlias,errorCode,reasonPhrase));
	       }

		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		@SuppressWarnings("rawtypes")
		LinkedHashMap res =  mapper.readValue(emList, LinkedHashMap.class);
		@SuppressWarnings({ "rawtypes", "unchecked" })
		String stat = (String) res.get("status");
		if(stat!=null)
		{

			System.out.println(new Date()+" : Dataflow {"+dataflowAlias+"} succesfully started");	
			return true;
		}
	    throw new IOException(String.format("Dataflow %s start failed: %s", dataflowAlias,emList));
	}
	
	public static void saveDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,df.getName()+".json");
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(dataflowFile, df);		
	}


	public static void createDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,df.getName()+".json");
		if(dataflowFile.exists())
		{
			throw new IllegalArgumentException("Dataflow {"+df.getName()+"} already exists in the system");
		}
		saveDataFlow(partnerConnection, df);
	}

	public static void copyDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		DataFlow df = getDataFlow(partnerConnection, dataflowAlias, dataflowId);
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,df.getName()+".json");
		dataflowFile = FileUtilsExt.getUniqueFile(dataflowFile);
		df.setName(FilenameUtils.getBaseName(dataflowFile.getName()));
		df.setMasterLabel(df.getMasterLabel() + " Copy");
		df.set_uid(null);
		df.setWorkflowType("Local");
		df.setLastModifiedBy(partnerConnection);
		saveDataFlow(partnerConnection, df);
	}

	
	public static void deleteDataFlow(PartnerConnection partnerConnection, String dataflowAlias) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,dataflowAlias+".json");
		if(dataflowFile.exists())
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			DataFlow _df = mapper.readValue(dataflowFile, DataFlow.class);
			if(_df.getWorkflowType().equalsIgnoreCase("local"))
			{
				java.nio.file.Files.delete(dataflowFile.toPath());
				if(dataflowFile.exists())
				{
					throw new IllegalArgumentException("Failed to delete Dataflow {"+dataflowAlias+"}");
				}
			}else
			{
				throw new IllegalArgumentException("Cannot delete Dataflow {"+dataflowAlias+"} of type {"+_df.getWorkflowType()+"}");
			}
		}else
		{
			throw new IllegalArgumentException("Dataflow {"+dataflowAlias+"} does not exist in the system");
		}
	}
	
	
	public static void upsertDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId, @SuppressWarnings("rawtypes") Map dataflowObject, boolean create, String dataflowMasterLabel) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		DataFlow df = null;
		if(!create)
		{
			df = getDataFlowLocal(partnerConnection, dataflowAlias);
			df.setMasterLabel(dataflowMasterLabel);
		}else
		{
			df = new DataFlow();
			df.set_type("Workflow");
			df.setName(dataflowAlias);
			df.setMasterLabel(dataflowMasterLabel);
			df.set_uid(dataflowId);
			df.setWorkflowType("Local");
			df.setLastModifiedBy(partnerConnection);
		}
		df.setWorkflowDefinition(dataflowObject);
		
		if(create)
		{
			createDataFlow(partnerConnection, df);
		}else
		{
			saveDataFlow(partnerConnection, df);
			if(!df.getWorkflowType().equalsIgnoreCase("local"))
			{
				uploadDataFlow(partnerConnection, df.getName(), df.get_uid(), df.getWorkflowDefinition());
			}
		}
	}
	
	public static List<DataFlow> listDataFlowLocal(PartnerConnection partnerConnection) throws ConnectionException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		List<DataFlow> list = new LinkedList<DataFlow>();
		File dir = DatasetUtilConstants.getDataflowDir(orgId);
		IOFileFilter suffixFileFilter = FileFilterUtils.suffixFileFilter(".json", IOCase.INSENSITIVE);
		File[] fileList =	DatasetUtils.getFiles(dir, suffixFileFilter);
		if(fileList!=null)
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 for(File file:fileList)
			 {
				 try {
					 DataFlow df = mapper.readValue(file, DataFlow.class);
						if(df.getName() != null && df.getWorkflowType().equalsIgnoreCase("local"))
						{
							list.add(df);
						}
				} catch (Exception e) {
					e.printStackTrace();
				}
			 }
		}
		return list;
	}
	
	public static DataFlow getDataFlowLocal(PartnerConnection partnerConnection, String dataFlowAlias) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,dataFlowAlias+".json");
		if(dataflowFile.exists())
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			return mapper.readValue(dataflowFile, DataFlow.class);	
		}
		return null;
	}




}
