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
package com.sforce.dataset.util;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;

/**
 * The Class DatasetDownloader.
 */
public class DatasetDownloader {

	/** The Constant nf. */
	public static final NumberFormat nf = NumberFormat.getIntegerInstance();
	
	/**
 * Gets the xmd.
 *
 * @param EM_NAME the em name
 * @param partnerConnection the partner connection
 * @return the xmd
 * @throws Exception the exception
 */
	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public static Map<String,String> getXMD(String EM_NAME, PartnerConnection partnerConnection) throws Exception 
	{
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String _alias = null;
		Date createdDateTime = null;
		String versionID = null;
		String id = null;
		
		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		String mainXmd = null;

		String serviceEndPoint = config.getServiceEndpoint();
		URI u = new URI(serviceEndPoint);
		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/edgemart", "current=true&alias="+EM_NAME,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();
		
		if(emList!=null && !emList.isEmpty())
		{
			try 
			{
				ObjectMapper mapper = new ObjectMapper();	
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				Map res =  mapper.readValue(emList, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
				List result = (List) res.get("result");
				if(result != null && !result.isEmpty())
				{
					Map resp = getAlias(result, EM_NAME);
					if(resp!=null)
					{
						_alias = (String) resp.get("_alias");
						id = (String) resp.get("_uid");
						Map edgemartData = (Map) resp.get("edgemartData");
						if(edgemartData != null)
						{
							versionID = (String) edgemartData.get("_uid");
						}
//						System.out.println("Found EM {"+_alias+"}, Version {"+versionID+"}, Created: {"+createdDateTime+"}");
						if(versionID!=null && id != null && !id.trim().isEmpty() && !versionID.trim().isEmpty())
							return getXMD(_alias, id, versionID, partnerConnection);
						else
							throw new IllegalArgumentException("Dataset {"+EM_NAME+"} not found");											
					}else
					{
						throw new IllegalArgumentException("Dataset {"+EM_NAME+"} not found");
					}
				}else
				{
					throw new IllegalArgumentException("Dataset {"+EM_NAME+"} not found");
				}
			} catch (Throwable t) {
				throw new IllegalArgumentException("Dataset {"+EM_NAME+"} not found: "+t.toString());
			}
		}else
		{
			throw new IllegalArgumentException("Dataset {"+EM_NAME+"} not found");
		}		
	}

	/**
	 * Gets the xmd.
	 *
	 * @param alias the alias
	 * @param datasetId the dataset id
	 * @param datasetVersion the dataset version
	 * @param partnerConnection the partner connection
	 * @return the xmd
	 * @throws Exception the exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String,String> getXMD(String alias, String datasetId,String datasetVersion, PartnerConnection partnerConnection) throws Exception 
	{
		if(datasetId == null || datasetVersion == null || datasetId.trim().isEmpty() || datasetVersion.trim().isEmpty())
			return getXMD(alias,partnerConnection);
		
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();		
		
		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		String mainXmd = null;
		   
		String serviceEndPoint = config.getServiceEndpoint();
		URI u = new URI(serviceEndPoint);

		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), String.format("/insights/internal_api/v1.0/esObject/edgemart/%s/version/%s/file/main.xmd.json",datasetId,datasetVersion),null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
	       if (statusCode != HttpStatus.SC_OK) {
	           System.out.println("Method failed: " + reasonPhrase);
		       throw new IllegalArgumentException(String.format("%s download failed: %d %s", "main.xmd.json",statusCode,reasonPhrase));
	       }

			HttpEntity emresponseEntity1 = emresponse.getEntity();
			InputStream emis1 = emresponseEntity1.getContent();
			String xmd = IOUtils.toString(emis1, "UTF-8");
			emis1.close();
			httpClient.close();

			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			Map xmdObject =  mapper.readValue(xmd, Map.class);
			mainXmd = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(xmdObject);
			LinkedHashMap map = new LinkedHashMap();
			map.put("datasetAlias", alias);
			map.put("datasetVersion", datasetVersion);
			map.put("datasetId", datasetId);
			map.put("mainXmd", mainXmd);
			return map;
	}
	
	
	/**
	 * Download em.
	 *
	 * @param EM_NAME the em name
	 * @param partnerConnection the partner connection
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean downloadEM(String EM_NAME, PartnerConnection partnerConnection) throws Exception 
	{
//			partnerConnection.getServerTimestamp();
			ConnectorConfig config = partnerConnection.getConfig();			
			String sessionID = config.getSessionId();
			String orgId = partnerConnection.getUserInfo().getOrganizationId();
			String _alias = null;
			Date createdDateTime = null;
			String versionID = null;
			
			String serviceEndPoint = config.getServiceEndpoint();
			CloseableHttpClient httpClient = HttpUtils.getHttpClient();
			RequestConfig requestConfig = HttpUtils.getRequestConfig();
			   
			URI u = new URI(serviceEndPoint);

			URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/edgemart", "current=true&alias="+EM_NAME,null);			
			HttpGet listEMPost = new HttpGet(listEMURI);

			listEMPost.setConfig(requestConfig);
			listEMPost.addHeader("Authorization","OAuth "+sessionID);			
			CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
			HttpEntity emresponseEntity = emresponse.getEntity();
			InputStream emis = emresponseEntity.getContent();			
			String emList = IOUtils.toString(emis, "UTF-8");
			emis.close();
			httpClient.close();
			
			if(emList!=null && !emList.isEmpty())
			{
				try 
				{
					ObjectMapper mapper = new ObjectMapper();	
					mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
					Map res =  mapper.readValue(emList, Map.class);
//					mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
					List result = (List) res.get("result");
					if(result != null && !result.isEmpty())
					{
						Map resp = getAlias(result, EM_NAME);
						if(resp!=null)
						{
							_alias = (String) resp.get("_alias");
							Integer _createdDateTime = (Integer) resp.get("_createdDateTime");
							//System.out.println("_createdDateTime: "+ _createdDateTime);
							if(_createdDateTime != null)
							{
								createdDateTime = new Date(1000L*_createdDateTime);
							}
//							Map folder = (Map) resp.get("folder");
//							if(folder != null)
//							{
//								folderID = (String) folder.get("_uid");
//							}
							Map edgemartData = (Map) resp.get("edgemartData");
							if(edgemartData != null)
							{
								versionID = (String) edgemartData.get("_uid");
							}
							System.out.println("Found EM {"+_alias+"}, Version {"+versionID+"}, Created: {"+createdDateTime+"}");
							System.out.println("Downloading EM File.. \n");
							Map _files = (Map) resp.get("_files");
							if(_files != null)
							{
								File edgemartDir = new File(DatasetUtilConstants.getDataDir(orgId),EM_NAME);
								FileUtils.forceMkdir(edgemartDir);
								for(Object filename:_files.keySet())
								{
									try
									{
									if(filename==null)
										continue;
											
									if(!DatasetUtilConstants.debug)
									{
										if(!filename.toString().endsWith("json"))
											continue;
									}
									
									CloseableHttpClient httpClient1 = HttpUtils.getHttpClient();
									String url = (String) _files.get(filename);
									URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), url, null,null);			
									HttpGet listEMPost1 = new HttpGet(listEMURI1);

									System.out.println("Downloading file {"+filename+"} + URI: " + listEMURI1);
									listEMPost1.setConfig(requestConfig);
									listEMPost1.addHeader("Authorization","OAuth "+sessionID);			

									long startTime = System.currentTimeMillis();
									long endTime = 0L;
									CloseableHttpResponse emresponse1 = httpClient1.execute(listEMPost1);

								   String reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
							       int statusCode = emresponse1.getStatusLine().getStatusCode();
							       if (statusCode != HttpStatus.SC_OK) {
							           System.out.println("Method failed: " + reasonPhrase);
								       System.out.println(String.format("%s download failed: %d %s", filename,statusCode,reasonPhrase));
							           continue;
							       }
//							       System.out.println(String.format("statusCode: %d %s", statusCode,reasonPhrase));
//							       System.out.println(String.format("reasonPhrase: %s", reasonPhrase));

									HttpEntity emresponseEntity1 = emresponse1.getEntity();
									InputStream emis1 = emresponseEntity1.getContent();
									File outfile = new File(edgemartDir,(String) filename);
									if(!filename.toString().endsWith("json"))
									{
//										System.out.println("file {"+outfile+"}. Content-length {"+emresponseEntity1.getContentLength()+"}");
										FileOutputStream out = new FileOutputStream(outfile);
										byte[] buffer = new byte[DatasetUtilConstants.DEFAULT_BUFFER_SIZE];											
										try
										{
											IOUtils.copyLarge(emis1, out, buffer);
											endTime = System.currentTimeMillis();
										}catch(Throwable t)
										{
											endTime = System.currentTimeMillis();
											t.printStackTrace();
										}finally
										{
											out.close();
											emis1.close();
											emresponse1.close();
											httpClient1.close();
										}
									}else
									{
										try
										{
											String xmd = IOUtils.toString(emis1, "UTF-8");
											endTime = System.currentTimeMillis();
											Map xmdObject =  mapper.readValue(xmd, Map.class);
											mapper.writerWithDefaultPrettyPrinter().writeValue(outfile, xmdObject);
										}catch(Throwable t)
										{
											endTime = System.currentTimeMillis();
											t.printStackTrace();
										}finally
										{
											emis1.close();
											httpClient1.close();
										}
									}
										System.out.println("file {"+outfile+"} downloaded. Size{"+nf.format(outfile.length())+"}, Time{"+nf.format(endTime-startTime)+"}\n");
									} catch (Throwable t) {
										t.printStackTrace();
									}
								} //end for
								System.out.println("Completed downloading Files for dataset {"+EM_NAME+"}");							
								return true;
							}else
							{
								System.out.println("\n Dataset {"+EM_NAME+"} does not have any files");							
								return false;
							}
						}else
						{
							System.out.println("\n Datset {"+EM_NAME+"} not found");							
							return false;
						}
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}

				//System.out.println(emList);
			}

			
		return false;
	}
	
	/**
	 * Gets the alias.
	 *
	 * @param emarts the emarts
	 * @param alias the alias
	 * @return the alias
	 */
	@SuppressWarnings("rawtypes")
	static Map getAlias(List<Map> emarts, String alias)
	{
		for(Map emart:emarts)
		{
			String _alias = (String) emart.get("_alias");
			if(_alias==null)
			{
				continue;
			}
			if(_alias != null && !_alias.isEmpty() && _alias.equals(alias))
			{
					return emart;
			}
		}
		return null;
	}
	
}

