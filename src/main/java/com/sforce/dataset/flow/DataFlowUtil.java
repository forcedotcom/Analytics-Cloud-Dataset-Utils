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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.util.HttpUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class DataFlowUtil {
	
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat defaultDateFormat = new SimpleDateFormat("EEEE MMMM d HH:mm:ss z yyyy"); //Mon Jun 15 00:12:03 GMT 2015
	public static final String dataflowURL = "/insights/internal_api/v1.0/esObject/workflow/%s/json";
	
	
//	public static final  SimpleDateFormat defaultDateFormat = (SimpleDateFormat) DateFormat.getDateTimeInstance(
//            DateFormat.FULL, 
//            DateFormat.FULL, 
//            Locale.getDefault());
	
	
	@SuppressWarnings("rawtypes")
	public static void uploadAndStartDataFlow(PartnerConnection partnerConnection, Map wfdef, String workflowName) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		DataFlow df = getDataFlow(partnerConnection, workflowName, null);
		if(df!=null)
		{
			uploadDataFlow(partnerConnection, df.name, df._uid, wfdef);
			startDataFlow(partnerConnection, df.name, df._uid);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void uploadDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId, Map dataflowObject) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		partnerConnection.getServerTimestamp();
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
       if (statusCode != HttpStatus.SC_OK) {
	       throw new IOException(String.format("Dataflow {%s} upload failed: %d %s", dataflowAlias,statusCode,reasonPhrase));
       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		@SuppressWarnings("unused")
		String emList = IOUtils.toString(emis, "UTF-8");
//		System.out.println(emList);
		emis.close();
		httpClient.close();
		System.out.println("Dataflow {"+dataflowAlias+"} successfully uploaded");

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static DataFlow getDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		DataFlow df = null;
		partnerConnection.getServerTimestamp();
		
//		if(dataflowId==null)
//		{
			List<DataFlow> dfList = listDataFlow(partnerConnection);
			for(DataFlow _df:dfList)
			{
				if(_df.name.equals(dataflowAlias))
				{
					dataflowId = _df._uid;
					df = _df;
				}
			}
//		}

		if(dataflowId== null || df == null)
		{
			throw new IllegalArgumentException("dataflowAlias {"+dataflowAlias+"} not found");
		}
		
//		if(df == null)
//		{
//			df = new DataFlow();
//			df.name = dataflowAlias;
//			df._uid = dataflowId;
//			df.WorkflowType = "User";
//		}
		
		String orgId = partnerConnection.getUserInfo().getOrganizationId();

		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		   
		URI u = new URI(serviceEndPoint);
		
		File dataDir = DatasetUtilConstants.getDataDir(orgId);
		
		File dataFlowFile = new File(dataDir,dataflowAlias+".json");

		URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), String.format(dataflowURL, dataflowId), null,null);			
		HttpGet listEMPost1 = new HttpGet(listEMURI1);
	
		listEMPost1.setConfig(requestConfig);
		listEMPost1.addHeader("Authorization","OAuth "+sessionID);			
	
		CloseableHttpResponse emresponse1 = httpClient.execute(listEMPost1);
	
		String reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
		int statusCode = emresponse1.getStatusLine().getStatusCode();
		if (statusCode != HttpStatus.SC_OK) {
			throw new IOException(String.format("Dataflow %s download failed: %d %s", dataFlowFile,statusCode,reasonPhrase));
		}
	
		HttpEntity emresponseEntity1 = emresponse1.getEntity();
		InputStream emis1 = emresponseEntity1.getContent();
		String dataFlowJson = IOUtils.toString(emis1, "UTF-8");								
		emis1.close();
		httpClient.close();

		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
						mapper.writerWithDefaultPrettyPrinter().writeValue(dataFlowFile, wfdef);
						df.workflowDefinition = wfdef;
						System.out.println("file {"+dataFlowFile+"} downloaded. Size{"+dataFlowFile.length()+"}");
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
		partnerConnection.getServerTimestamp();
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
	       if (statusCode != HttpStatus.SC_OK) {
		       throw new IOException(String.format("List Dataflow failed: %d %s", statusCode,reasonPhrase));
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
				List<Map> flows = (List<Map>) res.get("result");
				if(flows != null && !flows.isEmpty())
				{
					for(Map flow:flows)
					{
						String workflowType = (String) flow.get("WorkflowType");
						String name = (String) flow.get("name");
						if(name != null && workflowType != null)
						{
							DataFlow df = new DataFlow();
							df.name = name;
							df._uid = (String) flow.get("_uid");
							df._url = (String) flow.get("_url");
							df._type = (String) flow.get("_type");

							Object temp = flow.get("RefreshFrequencySec");
							if(temp != null && temp instanceof Number)
							{
								df.RefreshFrequencySec = ((Number)temp).intValue();
							}

							df.nextRun = (String) flow.get("nextRun");
							if(df.nextRun != null)
							{
								try {
//									System.out.println(defaultDateFormat.toPattern() + " : " + df.nextRun);
									df.nextRunTime = defaultDateFormat.parse(df.nextRun).getTime();
								} catch (ParseException e) {
									e.printStackTrace();
								}
							}
							df.MasterLabel = (String) flow.get("MasterLabel");
							df.WorkflowType = workflowType; 

							Map<String,String> temp1 = (Map<String, String>) flow.get("_lastModifiedBy");
							df._lastModifiedBy = DataFlow.getUserType(df, temp1);
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

		return dfList;
	}

	public static boolean startDataFlow(PartnerConnection partnerConnection, String dataflowAlias, String dataflowId) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		System.out.println();
		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		URI u = new URI(serviceEndPoint);

		URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), String.format(dataflowURL, dataflowId).replace("json", "start"), null,null);			
		
        HttpPut httput = new HttpPut(patchURI);
        httput.setConfig(requestConfig);
        httput.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(httput);
	   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
       int statusCode = emresponse.getStatusLine().getStatusCode();
       if (statusCode != HttpStatus.SC_OK) {
	       throw new IOException(String.format("Dataflow %s start failed: %d %s", dataflowAlias,statusCode,reasonPhrase));
       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();

		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		@SuppressWarnings("rawtypes")
		Map res =  mapper.readValue(emList, Map.class);
		@SuppressWarnings({ "rawtypes", "unchecked" })
		List<Map> resList = (List<Map>) res.get("result");
		if(resList!=null && !resList.isEmpty())
		{
			if((boolean) resList.get(0).get("success"))
			{
				System.out.println("Dataflow {"+dataflowAlias+"} succesfully started");	
				return true;
			}
		}
	    throw new IOException(String.format("Dataflow %s start failed: %s", dataflowAlias,emList));
	}
	
	public static void saveDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,df.name);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(dataflowFile, df);		
	}


	public static void createDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,df.name);
		if(dataflowFile.exists())
		{
			throw new IllegalArgumentException("Dataflow {"+df.name+"} already exists in the system");
		}
		saveDataFlow(partnerConnection, df);
	}


	public static void deleteDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dataflowDir = DatasetUtilConstants.getDataflowDir(orgId);
		File dataflowFile = new File(dataflowDir,df.name);
		if(dataflowFile.exists())
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			DataFlow _df = mapper.readValue(dataflowFile, DataFlow.class);
			if(_df.WorkflowType.equalsIgnoreCase("local"))
			{
				dataflowFile.delete();
			}else
			{
				throw new IllegalArgumentException("Cannot delete Dataflow {"+df.name+"} of type {"+df.WorkflowType+"}");
			}
		}else
		{
			throw new IllegalArgumentException("Dataflow {"+df.name+"} does not exist in the system");
		}
	}


}
