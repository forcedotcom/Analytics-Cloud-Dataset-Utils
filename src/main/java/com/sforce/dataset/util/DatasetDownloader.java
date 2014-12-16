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
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
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
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;

public class DatasetDownloader {
	

	/**
	 * @param binFile
	 * @param EM_NAME
	 * @param username
	 * @param password
	 * @param endpoint
	 * @param token
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static boolean downloadEM(String EM_NAME, PartnerConnection connection) throws Exception {

			ConnectorConfig config = connection.getConfig();			
			String sessionID = config.getSessionId();
			String _alias = null;
			Date createdDateTime = null;
			String versionID = null;
			String serviceEndPoint = config.getServiceEndpoint();
			CloseableHttpClient httpClient = HttpClients.createDefault();

			RequestConfig requestConfig = RequestConfig.custom()
				       .setSocketTimeout(60000)
				       .setConnectTimeout(60000)
				       .setConnectionRequestTimeout(60000)
				       .build();
			   
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
								File edgemartDir = new File(EM_NAME);
								FileUtils.forceMkdir(edgemartDir);
								for(Object filename:_files.keySet())
								{
									if(filename==null||!filename.toString().endsWith("json"))
										continue;
									
									CloseableHttpClient httpClient1 = HttpClients.createDefault();
									String url = (String) _files.get(filename);
									URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), url, null,null);			
									HttpGet listEMPost1 = new HttpGet(listEMURI1);

									System.out.println("Downloading file {"+filename+"}");
									listEMPost1.setConfig(requestConfig);
									listEMPost1.addHeader("Authorization","OAuth "+sessionID);			

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
									String xmd = IOUtils.toString(emis1, "UTF-8");
									emis1.close();
									httpClient1.close();

									Map xmdObject =  mapper.readValue(xmd, Map.class);
									File outfile = new File(edgemartDir,(String) filename);
									mapper.writerWithDefaultPrettyPrinter().writeValue(outfile, xmdObject);
//									System.out.println("file {"+outfile+"}. Content-length {"+emresponseEntity1.getContentLength()+"}");
//									BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outfile));
//									IOUtils.copy(emis1, out);
//									out.close();
									System.out.println("file {"+outfile+"} downloaded. Size{"+outfile.length()+"}\n");
								}
							}
							System.out.println("Completed downloading XMD Files..");							
							return true;
						}else
						{
							System.out.println("\n EM {"+EM_NAME+"} not found");							
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

	/*
	public static PartnerConnection login(final String username,
			String password, String token, String endpoint) throws Exception {
		
		if (username == null || username.isEmpty()) {
			throw new Exception("username is required");
		}

		if (password == null || password.isEmpty()) {
			throw new Exception("password is required");
		}

		if (endpoint == null || endpoint.isEmpty()) {
			throw new Exception("endpoint is required");
		}

		if (token == null)
			token = "";

		password = password + token;

		try {
			ConnectorConfig config = new ConnectorConfig();
			config.setUsername(username);
			config.setPassword(password);
			config.setAuthEndpoint(endpoint);
			config.setSessionRenewer(new SessionRenewerImpl(username, password, null, endpoint));

			PartnerConnection connection = new PartnerConnection(config);
			GetUserInfoResult userInfo = connection.getUserInfo();

			System.out.println("\nLogging in ...\n");
			//System.out.println("UserID: " + userInfo.getUserId());
			//System.out.println("User Full Name: " + userInfo.getUserFullName());
			System.out.println("User Email: " + userInfo.getUserEmail());
			//System.out.println("SessionID: " + config.getSessionId());
			//System.out.println("Auth End Point: " + config.getAuthEndpoint());
			//System.out.println("Service End Point: " + config.getServiceEndpoint());
			
			return connection;

		} catch (ConnectionException e) {
			e.printStackTrace();
			throw new Exception(e.getLocalizedMessage());
		}
	}
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

