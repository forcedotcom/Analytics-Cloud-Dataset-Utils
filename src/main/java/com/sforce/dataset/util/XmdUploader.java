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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class XmdUploader {
	

	/**
	 * @param userXmd
	 * @param EM_NAME
	 * @param username
	 * @param password
	 * @param endpoint
	 * @param token
	 * @throws ConnectionException 
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean uploadXmd(String userXmdFile, String datasetAlias, PartnerConnection connection) throws URISyntaxException, ClientProtocolException, IOException, ConnectionException
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
			
			ConnectorConfig config = connection.getConfig();			
			String sessionID = config.getSessionId();
			String _alias = null;
			String edgemartId = null;
			Date createdDateTime = null;
			String folderID = null;
			String versionID = null;
			String userXmdUri = null;
			String serviceEndPoint = config.getServiceEndpoint();
			CloseableHttpClient httpClient = HttpClients.createDefault();
			
			URI u = new URI(serviceEndPoint);

			URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/edgemart", "current=true",null);			
			HttpGet listEMPost = new HttpGet(listEMURI);

//			MultipartEntityBuilder builder = MultipartEntityBuilder.create();
//			builder.addTextBody("jsonMetadata", "{\"_alias\":\""+EM_NAME+"\",\"_type\":\"ebin\"}", ContentType.TEXT_PLAIN);
//			builder.addBinaryBody(binFile.getName(), binFile,
//					ContentType.APPLICATION_OCTET_STREAM, binFile.getName());
//			HttpEntity multipart = builder.build();
//
//			uploadFile.setEntity(multipart);
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
							edgemartId = (String) resp.get("_uid");
							
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
								versionID = (String) edgemartData.get("_uid");
							}
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
//						System.out.println(resp);
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}

				//System.out.println(emList);
			}

			if(_alias != null && _alias.equals(datasetAlias))
			{
				System.out.println("Found existing Dataset {"+_alias+"} version {"+versionID+"}, created on {"+createdDateTime+"}, in folder {"+folderID+"}");
			}else
			{
				System.out.println("Dataset {"+_alias+"} not found");
				return false;
			}
			
			String altuserXmdUri = "/insights/internal_api/v1.0/esObject/edgemart/"+edgemartId+"/version/"+versionID+"/file/user.xmd.json"; 
			if(userXmdUri==null || userXmdUri.isEmpty())
				userXmdUri = altuserXmdUri;
//			else
//				System.out.println("xmd uri match: " + userXmdUri.equals(altuserXmdUri));

//			URI uploadURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/externaldata", null,null);
			URI uploadURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(),userXmdUri, null,null);
//			System.out.println("upload URI: " +  uploadURI);			
			HttpPost uploadFile = new HttpPost(uploadURI);

			MultipartEntityBuilder builder = MultipartEntityBuilder.create();
//			builder.addTextBody("jsonMetadata", "{\""+name_param+"\":\""+EM_NAME+"\",\"_type\":\""+format+"\"}", ContentType.TEXT_PLAIN);
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
//					mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res); //_type=edgemartdatafile, _uid=0FdB00000005ZfcKAE}
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
								System.out.println("User XMD succesfully uploaded to Dataset {"+_alias+"},  version {"+versionID+"}");
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
//						if(name_param.equals("name"))
//							System.out.println("EM {"+emName+"} : is being created");
//						else
//							System.out.println("EM {"+emName+"} : is being updated");
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

