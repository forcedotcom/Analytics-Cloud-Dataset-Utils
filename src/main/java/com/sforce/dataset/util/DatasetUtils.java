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
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.sql.RowId;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.LoginFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class DatasetUtils {
	
	private static final int EOF = -1;
	private static final char LF = '\n';
	private static final char CR = '\r';
	private static final char QUOTE = '"';
	private static final char COMMA = ',';

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 4;
	private static final  SimpleDateFormat sfdcDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	private static final Charset utf8Charset = Charset.forName("UTF-8");

	
	private static boolean hasLoggedIn = false;

	@SuppressWarnings("rawtypes")
	public static Map<String,Map> listPublicDataset(PartnerConnection connection) throws Exception {
		GetUserInfoResult userInfo = connection.getUserInfo();
		String userID = userInfo.getUserId();
		Map<String, Map> dataSetMap = listDataset(connection);
		if(dataSetMap==null || dataSetMap.size()==0)
		{
			return dataSetMap;
		}
		LinkedHashMap<String,Map> publicDataSetMap = new LinkedHashMap<String,Map>();
		for(String alias:dataSetMap.keySet())
		{
			Map resp = dataSetMap.get(alias);
			Map folder = (Map) resp.get("folder");
			if(folder != null)
			{
				String folderID = (String) folder.get("_uid");
				if(!folderID.equals(userID))
					publicDataSetMap.put(alias, resp);
			}

		}
		return publicDataSetMap;

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String,Map> listDataset(PartnerConnection connection) throws Exception {
		LinkedHashMap<String,Map> dataSetMap = new LinkedHashMap<String,Map>();
		ConnectorConfig config = connection.getConfig();			
		String sessionID = config.getSessionId();
//		String _alias = null;
//		Date createdDateTime = null;
//		String versionID = null;
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
				List<Map> result = (List) res.get("result");
				if(result != null && !result.isEmpty())
				{
					for(Map emart:result)
					{
						String _alias = (String) emart.get("_alias");
						if(_alias != null && !_alias.isEmpty())
						{
							dataSetMap.put(_alias, emart);
						}
					}
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}

			//System.out.println(emList);
		}

		
	return dataSetMap;
}

	
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
									CloseableHttpClient httpClient1 = HttpClients.createDefault();
									String url = (String) _files.get(filename);
									URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), url, null,null);			
									HttpGet listEMPost1 = new HttpGet(listEMURI1);

									System.out.println("Downloading file {"+filename+"} from url {"+listEMURI1+"}");
									listEMPost1.setConfig(requestConfig);
									listEMPost1.addHeader("Authorization","OAuth "+sessionID);			

									CloseableHttpResponse emresponse1 = httpClient1.execute(listEMPost1);

								   String reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
							       int statusCode = emresponse1.getStatusLine().getStatusCode();
							       if (statusCode != HttpStatus.SC_OK) {
							           System.out.println("Method failed: " + reasonPhrase);
							           continue;
							       }
							       System.out.println(String.format("statusCode: %d", statusCode));
							       System.out.println(String.format("reasonPhrase: %s", reasonPhrase));

									HttpEntity emresponseEntity1 = emresponse1.getEntity();
									InputStream emis1 = emresponseEntity1.getContent();
									File outfile = new File(edgemartDir,(String) filename);
									System.out.println("file {"+outfile+"}. Content-length {"+emresponseEntity1.getContentLength()+"}");
									BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outfile));
									IOUtils.copy(emis1, out);
									out.close();
									emis1.close();
									httpClient1.close();
									System.out.println("file {"+outfile+"} downloaded. Size{"+outfile.length()+"}");
								}
							}
							System.out.println("\n Completed downloading EM Files..");							
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

	
	public static PartnerConnection login(int retryCount,String username,String password, String token, String endpoint, String sessionId) throws ConnectionException  {

		if(sessionId==null)
		{
			if (username == null || username.isEmpty()) {
				throw new IllegalArgumentException("username is required");
			}
	
			if (password == null || password.isEmpty()) {
				throw new IllegalArgumentException("password is required");
			}

			if (token != null)
				password = password + token;
		}

		if (endpoint == null || endpoint.isEmpty()) {
			throw new IllegalArgumentException("endpoint is required");
		}


		try {
			ConnectorConfig config = new ConnectorConfig();
			if(sessionId!=null)
			{
			    config.setServiceEndpoint(endpoint);
			    config.setSessionId(sessionId);
			}else
			{
				config.setUsername(username);
				config.setPassword(password);
				config.setAuthEndpoint(endpoint);
				config.setSessionRenewer(new SessionRenewerImpl(username, password, null, endpoint));
			}
			PartnerConnection connection = new PartnerConnection(config);
			if(!hasLoggedIn)
			{
				System.out.println("\nLogging in ...");
				@SuppressWarnings("unused")
				GetUserInfoResult userInfo = connection.getUserInfo();
				System.out.println("Service Endpoint: " + config.getServiceEndpoint());
				System.out.println("SessionId Endpoint: " + config.getSessionId());
//				System.out.println("User Id: " + userInfo.getUserName());
//				System.out.println("User Email: " + userInfo.getUserEmail());
				System.out.println();
				hasLoggedIn = true;
			}
			return connection;
		}catch (ConnectionException e) {	
			System.out.println(e.getClass().getCanonicalName());
			e.printStackTrace();
			boolean retryError = true;	
			if(e instanceof LoginFault || sessionId != null)
				retryError = false;
			if(retryCount<3 && retryError)
			{
				retryCount++;
				try {
					Thread.sleep(1000*retryCount);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				return login(retryCount,username, password, null, endpoint, sessionId);
			}
			throw new ConnectionException(e.toString());
		}
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
	
	
	 public static <K, V extends Comparable<? super V>> Map<K, V> 
     sortByValue( Map<K, V> map )
 {
     List<Map.Entry<K, V>> list =
         new LinkedList<Map.Entry<K, V>>( map.entrySet() );
     Collections.sort( list, new Comparator<Map.Entry<K, V>>()
     {
         public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
         {
             return (o1.getValue()).compareTo( o2.getValue() );
         }
     } );

     Map<K, V> result = new LinkedHashMap<K, V>();
     for (Map.Entry<K, V> entry : list)
     {
         result.put( entry.getKey(), entry.getValue() );
     }
     return result;
 }
	 
	 
	public static String readInputFromConsole(String prompt) {
		String line = null;
		Console c = System.console();
		if (c != null) {
			line = c.readLine(prompt);
		} else {
			System.out.print(prompt);
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(System.in));
			try {
				line = bufferedReader.readLine();
			} catch (IOException e) {
				// Ignore
			}
		}
		return line;
	}
	
	public static String readPasswordFromConsole(String prompt) {
		String line = null;
		Console c = System.console();
		if (c != null) {
			char[] tmp = c.readPassword(prompt);
			if(tmp!=null)
				line = new String(tmp);
		} else {
			System.out.print(prompt);
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(System.in));
			try {
				line = bufferedReader.readLine();
			} catch (IOException e) {
				// Ignore
			}
		}
		return line;
	}

	
	public static String replaceSpecialCharacters(String inString) {
		String outString = inString;
		try 
		{
			if(inString != null && !inString.trim().isEmpty())
			{
				char[] outStr = new char[inString.length()];
				int index = 0;
				for(char ch:inString.toCharArray())
				{
					if(!Character.isLetterOrDigit((int)ch))
					{
						outStr[index] = '_';
					}else
					{
						outStr[index] = ch;
					}
					index++;
				}
				outString = new String(outStr);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return outString;
	}
	
	public static String getCSVFriendlyString(String content)
	{
		if(content!=null && !content.isEmpty())
		{
		content = replaceString(content, "" + COMMA, "");
		content = replaceString(content, "" + CR, "");
		content = replaceString(content, "" + LF, "");
		content = replaceString(content, "" + QUOTE, "");
		}
		return content;
	}

	
	private static String replaceString(String original, String pattern, String replace) 
	{
		if(original != null && !original.isEmpty() && pattern != null && !pattern.isEmpty() && replace !=null)
		{
			final int len = pattern.length();
			int found = original.indexOf(pattern);

			if (found > -1) {
				StringBuffer sb = new StringBuffer();
				int start = 0;

				while (found != -1) {
					sb.append(original.substring(start, found));
					sb.append(replace);
					start = found + len;
					found = original.indexOf(pattern, start);
				}

				sb.append(original.substring(start));

				return sb.toString();
			} else {
				return original;
			}
		}else
			return original;
	}
	
	
	public  static Object toJavaPrimitiveType(Object value) throws UnsupportedEncodingException 
	{
		if(value==null)
			return value;
				
		if(value instanceof InputStream)
		{
			try {
				value = new String(toBytes((InputStream)value), "UTF-8"); ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else if(value instanceof Reader)
		{
			value = toString((Reader) value);
		}
		if(value instanceof UUID)
		{
			value = ((UUID)value).toString();
		}else 
		if(value instanceof RowId)
		{
			value = ((RowId)value).toString();		
		}else if(value instanceof byte[])
		{
			value = new String(com.sforce.ws.util.Base64.encode((byte[]) value), "UTF-8"); ;
		}else if(value instanceof Date)
		{
			value = sfdcDateTimeFormat.format((Date)value);	
		}else if(value instanceof java.sql.Timestamp)
		{
			value = sfdcDateTimeFormat.format((java.sql.Timestamp)value);	
		}else
		{
			value = value.toString();
		}
		
		return value;
	}

	
	/**
	 * Converts Reader to String
	 * @param the input reader
	 * @return the string read from the reader
	 */
	public static String toString(Reader input) {
		if(input == null)
			return null;
		try {
			StringBuffer sbuf = new StringBuffer();
			char[] cbuf = new char[DEFAULT_BUFFER_SIZE];
			int count = -1;
			try {
				int n;
				while ((n = input.read(cbuf)) != -1)
				{
					sbuf.append(cbuf, 0, n);
					count = ((count == -1) ? n : (count + n));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (count == -1)
				return null;
			else
				return sbuf.toString();
		} finally {
			if (input != null)
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
	/**
	 * Converts an Input Stream to byte[]
	 * @param the input stream
	 * @return the byte[] read from the input stream
	 * @throws IOException
	 */
	public static byte[] toBytes(InputStream input) throws IOException {
		if(input == null)
			return null;
		try 
		{
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
		int n = 0;
		int count = -1;
		while (EOF != (n = input.read(buffer))) {
			output.write(buffer, 0, n);
			count = ((count == -1) ? n : (count + n));
		}
		output.flush();
		if(count == -1)
			return null;
		else
			return output.toByteArray();
		} finally {
			if (input != null)
				try {
					input.close();
				} catch (IOException e) {e.printStackTrace();}
		}
	} 
	
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }


    public static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);  
   }

   public static String padLeft(String s, int n) {
       return String.format("%1$" + n + "s", s);  
   }
   
   
	public static CharsetEncoder utf8Encoder(CodingErrorAction codingErrorAction) {
	    try 
	    {
			if(codingErrorAction==null)
				codingErrorAction = CodingErrorAction.REPORT;
		    final CharsetEncoder encoder = utf8Charset.newEncoder();
		    encoder.reset();
		    encoder.onUnmappableCharacter(codingErrorAction);
		    encoder.onMalformedInput(codingErrorAction);
	        return encoder;
	    } catch (Throwable t) {
	    	t.printStackTrace();
	    	return null;
	    }
}

public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction, Charset fileCharset) {
    try 
    {
    	if(fileCharset == null)
    		fileCharset = utf8Charset;
		if(codingErrorAction==null)
			codingErrorAction = CodingErrorAction.IGNORE;
	    final CharsetDecoder encoder = fileCharset.newDecoder();
	    encoder.reset();
	    encoder.onUnmappableCharacter(codingErrorAction);
	    encoder.onMalformedInput(codingErrorAction);
        return encoder;
    } catch (Throwable t) {
    	t.printStackTrace();
    	return null;
    }
}

public static byte[] toBytes(String value, CodingErrorAction codingErrorAction) {
	if(value != null)
	{
	    try 
	    {
			if(codingErrorAction==null)
				codingErrorAction = CodingErrorAction.IGNORE;
		    final CharsetEncoder encoder = utf8Encoder(codingErrorAction);
	        final ByteBuffer b = encoder.encode(CharBuffer.wrap(value));
	        final byte[] bytes = new byte[b.remaining()];
	        b.get(bytes);
	        return bytes;
	    } catch (Throwable t) {
	    	t.printStackTrace();
	    	return value.getBytes(utf8Charset);
	    }
	}
    return null;
}

	
}

