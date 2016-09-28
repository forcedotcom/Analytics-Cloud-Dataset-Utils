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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * The Class XmdUploader.
 */
public class XmdUploader {
	

	/**
	 * Upload xmd.
	 *
	 * @param userXmdFile the user xmd file
	 * @param datasetAlias the dataset alias
	 * @param datasetId the dataset id
	 * @param datasetVersion the dataset version
	 * @param partnerConnection the partner connection
	 * @return true, if successful
	 * @throws URISyntaxException the URI syntax exception
	 * @throws ClientProtocolException the client protocol exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ConnectionException the connection exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean uploadXmd(String userXmdFile, String datasetAlias, String datasetId,String datasetVersion, PartnerConnection partnerConnection) throws URISyntaxException, ClientProtocolException, IOException, ConnectionException
	{
		if(datasetAlias==null||datasetAlias.trim().isEmpty())
		{
			throw new IllegalArgumentException("datasetAlias cannot be blank");
		}
		
		if(userXmdFile==null || userXmdFile.trim().isEmpty() )
		{
			throw new IllegalArgumentException("xmdFile cannot be blank");
		}

		File userXmd = new File(userXmdFile);
		if(userXmd==null || !userXmd.exists() || !userXmd.canRead())
		{
			throw new IllegalArgumentException("cannot open file {"+userXmd+"}");
		}
		
		try
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			@SuppressWarnings("unused")
			Map res =  mapper.readValue(userXmd, Map.class);
		}catch(Throwable t)
		{
			throw new IllegalArgumentException("{"+userXmd+"} is not valid json, Error: " + t.getMessage());
//			t.printStackTrace();
		}


			System.out.println();
//			partnerConnection.getServerTimestamp();			
			ConnectorConfig config = partnerConnection.getConfig();			
			String sessionID = config.getSessionId();
			String _alias = null;
			Date createdDateTime = null;
			String folderID = null;
			String userXmdUri = null;
			String serviceEndPoint = config.getServiceEndpoint();

			CloseableHttpClient httpClient = HttpUtils.getHttpClient();
			RequestConfig requestConfig = HttpUtils.getRequestConfig();
			
			URI u = new URI(serviceEndPoint);

			if(datasetId == null || datasetId.trim().equalsIgnoreCase("null") || datasetVersion == null || datasetVersion.trim().equalsIgnoreCase("null") || datasetId.trim().isEmpty() || datasetVersion.trim().isEmpty())
			{

			URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/edgemart", "current=true&alias="+datasetAlias,null);			
			HttpGet listEMPost = new HttpGet(listEMURI);
			listEMPost.setConfig(requestConfig);
			
			listEMPost.addHeader("Authorization","OAuth "+sessionID);			
			CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
			HttpEntity emresponseEntity = emresponse.getEntity();
			InputStream emis = emresponseEntity.getContent();
			String emList = IOUtils.toString(emis, "UTF-8");
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
						Map resp = getAlias(result, datasetAlias);
						if(resp!=null)
						{
							_alias = (String) resp.get("_alias");
							datasetId = (String) resp.get("_uid");
							
							Integer _createdDateTime = (Integer) resp.get("_createdDateTime");
							//System.out.println("_createdDateTime: "+ _createdDateTime);
							if(_createdDateTime != null)
							{
								createdDateTime = new Date(1000L*_createdDateTime);
							}
							Map folder = (Map) resp.get("folder");
							if(folder != null)
							{
								folderID = (String) folder.get("_uid");
							}
							Map edgemartData = (Map) resp.get("edgemartData");
							if(edgemartData != null)
							{
								datasetVersion = (String) edgemartData.get("_uid");
							}
						
							Map _files = (Map) resp.get("_files");
							if(_files != null)
							{
								for(Object filename:_files.keySet())
								{
									if(filename!=null && filename.toString().equals("user.xmd.json"))
									{
											userXmdUri = (String) _files.get(filename);
											break;
									}
								}
							}
						}
//						System.out.println(resp);
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}

				//System.out.println(emList);
			}

			if(_alias != null && _alias.equals(datasetAlias))
			{
				System.out.println("Found existing Dataset {"+_alias+"} version {"+datasetVersion+"}, created on {"+createdDateTime+"}, in folder {"+folderID+"}");
			}else
			{
				System.out.println("Dataset {"+datasetAlias+"} not found");
				return false;
			}
			
			}
			
			String altuserXmdUri = "/insights/internal_api/v1.0/esObject/edgemart/"+datasetId+"/version/"+datasetVersion+"/file/user.xmd.json"; 
			if(userXmdUri==null || userXmdUri.isEmpty())
				userXmdUri = altuserXmdUri;
			
			if(!altuserXmdUri.equals(userXmdUri))
			{
				System.err.println("xmd url is different: ");
				System.err.println(altuserXmdUri);
				System.err.println(userXmdUri);
			}

			URI uploadURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(),altuserXmdUri, null,null);
			HttpPost uploadFile = new HttpPost(uploadURI);

			MultipartEntityBuilder builder = MultipartEntityBuilder.create();
			builder.addBinaryBody("user.xmd.json", userXmd,
					ContentType.APPLICATION_OCTET_STREAM, "user.xmd.json");
			HttpEntity multipart = builder.build();

			uploadFile.setEntity(multipart);
			uploadFile.addHeader("Authorization","OAuth "+sessionID);			
			CloseableHttpResponse response = httpClient.execute(uploadFile);
			HttpEntity responseEntity = response.getEntity();
			InputStream is = responseEntity.getContent();
			String responseString = IOUtils.toString(is, "UTF-8");
			if(responseString!=null && !responseString.trim().isEmpty() )
			{
				try 
				{
					ObjectMapper mapper = new ObjectMapper();
					mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
					Map<String, List<Map<String, String>>> res =  (Map<String, List<Map<String, String>>>)mapper.readValue(responseString, Map.class);
					List<Map<String, String>> result = res.get("result");
					if(result != null && !result.isEmpty())
					{
						Map<String, String> resp = result.get(0);
						if(resp!=null && !resp.isEmpty())
						{
							@SuppressWarnings("unused")
							String _type = resp.get("_type");
							String _uid = resp.get("_uid");
							if(_uid!=null)
							{
								System.out.println("User XMD succesfully uploaded to Dataset {"+_alias+"},  version {"+datasetVersion+"}");
								return true;
							}else
							{
								System.out.println("User XMD uploaded to Dataset {"+_alias+"} failed");
								System.out.println(resp);
								return false;
							}
						}else
						{
							System.out.println("User XMD uploaded to Dataset {"+_alias+"} failed");
							System.out.println(res);
							return false;
						}
					}else
					{
						System.out.println("User XMD uploaded to Dataset {"+_alias+"} failed");
						System.out.println(responseString);
						return false;
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}
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

