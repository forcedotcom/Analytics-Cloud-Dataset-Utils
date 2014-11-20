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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
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
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class DataFlowUtil {
	
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	
	@SuppressWarnings("rawtypes")
	public static void uploadAndStartDataFlow(PartnerConnection partnerConnection, Map wfdef, String workflowName) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		DataFlow df = getDataFlow(partnerConnection, workflowName);
		if(df!=null)
		{
			df.workflowDefinition = wfdef;
			uploadDataFlow(partnerConnection, df);
			startDataFlow(partnerConnection, df);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void uploadDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		System.out.println();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();
		CloseableHttpClient httpClient = HttpClients.createDefault();

		RequestConfig requestConfig = RequestConfig.custom()
			       .setSocketTimeout(60000)
			       .setConnectTimeout(60000)
			       .setConnectionRequestTimeout(60000)
			       .build();
		   
		URI u = new URI(serviceEndPoint);

		URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), df._url, null,null);			
		
        HttpPatch httpPatch = new HttpPatch(patchURI);
//        httpPatch.addHeader("Accept", "*/*");
//        httpPatch.addHeader("Content-Type", "application/json");
        
		Map map = new LinkedHashMap();
		map.put("workflowDefinition", df.workflowDefinition);
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
	       throw new IOException(String.format("Dataflow {%s} upload failed: %d %s", df.name,statusCode,reasonPhrase));
       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		@SuppressWarnings("unused")
		String emList = IOUtils.toString(emis, "UTF-8");
//		System.out.println(emList);
		emis.close();
		httpClient.close();
		System.out.println("Dataflow {"+df.name+"} successfully uploaded");

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static DataFlow getDataFlow(PartnerConnection partnerConnection, String workflowName) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		System.out.println();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();
		CloseableHttpClient httpClient = HttpClients.createDefault();

		RequestConfig requestConfig = RequestConfig.custom()
			       .setSocketTimeout(60000)
			       .setConnectTimeout(60000)
			       .setConnectionRequestTimeout(60000)
			       .build();
		   
		URI u = new URI(serviceEndPoint);

		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/workflow", null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

//		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
//		builder.addTextBody("jsonMetadata", "{\"_alias\":\""+EM_NAME+"\",\"_type\":\"ebin\"}", ContentType.TEXT_PLAIN);
//		builder.addBinaryBody(binFile.getName(), binFile,
//				ContentType.APPLICATION_OCTET_STREAM, binFile.getName());
//		HttpEntity multipart = builder.build();
//
//		uploadFile.setEntity(multipart);
		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
	       if (statusCode != HttpStatus.SC_OK) {
		       throw new IOException(String.format("Dataflow {%s} download failed: %d %s", workflowName,statusCode,reasonPhrase));
	       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();
		
		if(emList!=null && !emList.isEmpty())
		{
//			try 
//			{
				ObjectMapper mapper = new ObjectMapper();	
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				Map res =  mapper.readValue(emList, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
				List<Map> flows = (List<Map>) res.get("result");
				if(flows != null && !flows.isEmpty())
				{
					for(Map flow:flows)
					{
						String workflowType = (String) flow.get("WorkflowType");
						String name = (String) flow.get("name");
						if(name != null && workflowType != null && workflowType.equals("User") && name.equals(workflowName))
						{
							DataFlow df = new DataFlow();
							df.name = name;
							df._type = workflowType;
							df._uid = (String) flow.get("_uid");
							df._url = (String) flow.get("_url");
							df._type = (String) flow.get("_type");
							df.RefreshFrequencySec = (Integer) flow.get("RefreshFrequencySec");
							df.nextRun = (String) flow.get("nextRun");
							df.MasterLabel = (String) flow.get("MasterLabel");
							
							if(df._url!=null)
							{				
								File dataDir = new File(df.name);
								try
								{
									FileUtils.forceMkdir(dataDir);
								}catch(Throwable t)
								{
									t.printStackTrace();
								}
//								File dataflowFile = new File(dataDir,df.name+".json");
//								
								File dataFlowFile = new File(dataDir,df.name+"_"+sdf.format(new Date())+".json");
								CloseableHttpClient httpClient1 = HttpClients.createDefault();
								URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), df._url, null,null);			
								HttpGet listEMPost1 = new HttpGet(listEMURI1);
	
	//							System.out.println("Downloading file {"+filename+"} from url {"+listEMURI1+"}");
//								System.out.println("Downloading file {"+dataFlowFile+"}");
								listEMPost1.setConfig(requestConfig);
								listEMPost1.addHeader("Authorization","OAuth "+sessionID);			
	
								CloseableHttpResponse emresponse1 = httpClient1.execute(listEMPost1);
	
							   reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
						        statusCode = emresponse1.getStatusLine().getStatusCode();
						       if (statusCode != HttpStatus.SC_OK) {
//						           System.err.println("Method failed: " + reasonPhrase);
							       throw new IOException(String.format("Dataflow %s download failed: %d %s", dataFlowFile,statusCode,reasonPhrase));
						       }
	//					       System.out.println(String.format("statusCode: %d %s", statusCode,reasonPhrase));
	//					       System.out.println(String.format("reasonPhrase: %s", reasonPhrase));
	
								HttpEntity emresponseEntity1 = emresponse1.getEntity();
								InputStream emis1 = emresponseEntity1.getContent();
//								System.out.println("file {"+dataFlowFile+"}. Content-length {"+emresponseEntity1.getContentLength()+"}");
								String dataFlowJson = IOUtils.toString(emis1, "UTF-8");								
								emis1.close();
								httpClient1.close();
								
								if(dataFlowJson!=null && !dataFlowJson.isEmpty())
								{
//									try 
//									{
										Map res2 =  mapper.readValue(dataFlowJson, Map.class);
//										mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
										List<Map> flows2 = (List<Map>) res2.get("result");
										if(flows != null && !flows.isEmpty())
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
//									} catch (Throwable t) {
//										t.printStackTrace();
//									}
								}else
								{
								       throw new IOException(String.format("Dataflow download failed, invalid server response %s",dataFlowJson));
								}
//								BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dataFlowFile));
//								JsonNode node = mapper.readTree(df.workflowDefinition);
//								mapper.writerWithDefaultPrettyPrinter().writeValue(dataFlowFile, node);
//								df.workflowDefinition = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
//								IOUtils.copy(emis1, out);
//								out.close();
							}else
							{
						       throw new IOException(String.format("Dataflow download failed, invalid server response %s",emList));
							}
							return df;
						}else
						{
					       throw new IOException(String.format("Dataflow download failed, invalid server response %s",emList));
						}
					} //end for
				}else
				{
			       throw new IOException(String.format("Dataflow download failed, invalid server response %s",emList));
				}
//			} catch (Throwable t) {
//				t.printStackTrace();
//			}
			//System.err.println(emList);
		}
		System.err.println("Dataflow {"+workflowName+"} not found");
		return null;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void startDataFlow(PartnerConnection partnerConnection, DataFlow df) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		System.out.println();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();
		CloseableHttpClient httpClient = HttpClients.createDefault();

		RequestConfig requestConfig = RequestConfig.custom()
			       .setSocketTimeout(60000)
			       .setConnectTimeout(60000)
			       .setConnectionRequestTimeout(60000)
			       .build();
		   
		URI u = new URI(serviceEndPoint);

		URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), df._url.replace("json", "start"), null,null);			
		
        HttpPut httpPatch = new HttpPut(patchURI);
//        httpPatch.addHeader("Accept", "*/*");
//        httpPatch.addHeader("Content-Type", "application/json");
        
		Map map = new LinkedHashMap();
		map.put("_uid", df._uid);
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
	       throw new IOException(String.format("Dataflow %s start failed: %d %s", df.name,statusCode,reasonPhrase));
       }
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		@SuppressWarnings("unused")
		String emList = IOUtils.toString(emis, "UTF-8");
//		System.out.println(emList);
		System.out.println("Dataflow {"+df.name+"} succesfully started");
		emis.close();
		httpClient.close();

	}



}
