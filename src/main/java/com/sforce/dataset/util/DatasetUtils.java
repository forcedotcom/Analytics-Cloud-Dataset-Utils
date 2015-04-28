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
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
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
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
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
	
	public static final Charset utf8Charset = Charset.forName("UTF-8");

	
	private static boolean hasLoggedIn = false;

	@SuppressWarnings("rawtypes")
	public static Map<String,Map> listPublicDataset(PartnerConnection connection, boolean isCurrent) throws Exception {
		GetUserInfoResult userInfo = connection.getUserInfo();
		String userID = userInfo.getUserId();
		Map<String, Map> dataSetMap = listDataset(connection, isCurrent);
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
	public static Map<String,Map> listDataset(PartnerConnection partnerConnection, boolean isCurrent) throws Exception {
		LinkedHashMap<String,Map> dataSetMap = new LinkedHashMap<String,Map>();
		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
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

		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/edgemart", "current="+isCurrent+"&sortOrder=Mru",null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String emList = IOUtils.toString(emis, "UTF-8");
//		System.out.println("Response Size:"+emList.length());
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


	@SuppressWarnings("rawtypes")
	public static List<DatasetType> listDatasets(PartnerConnection connection, boolean isCurrent) throws Exception 
	{
		List<DatasetType> datasetList = new LinkedList<DatasetType>();
		Map<String, Map> dataSetMap = listDataset(connection, isCurrent);
		if(dataSetMap != null && !dataSetMap.isEmpty())
		{
			for(String alias:dataSetMap.keySet())
			{
				Map dataset = dataSetMap.get(alias);
				String _type = (String) dataset.get("_type");
				if(_type != null && _type.equals("edgemart"))
				{
						DatasetType datasetTemp = new DatasetType();
						
						Object temp = dataset.get("_createdDateTime");
						if(temp != null && temp instanceof Number)
						{
							datasetTemp._createdDateTime = ((Number)temp).longValue();
						}

						temp = dataset.get("_lastAccessed");
						if(temp != null && temp instanceof Number)
						{
							datasetTemp._lastAccessed = ((Number)temp).longValue();
						}

						datasetTemp._type  = (String) dataset.get("_type");
						
						datasetTemp._uid  = (String) dataset.get("_uid");
						
						datasetTemp.assetIcon = (String) dataset.get("_type");
						
						datasetTemp.assetIconUrl = (String) dataset.get("assetIconUrl");
						
						datasetTemp.assetSharingUrl = (String) dataset.get("assetSharingUrl");
						
						datasetTemp._alias = (String) dataset.get("_alias");

						datasetTemp.name = (String) dataset.get("name");
						
						datasetTemp._url = (String) dataset.get("_url");

						temp =  dataset.get("_permissions");
						if(temp != null && temp instanceof Map)
						{
							datasetTemp._permissions = datasetTemp.new PermissionType();
							Object var = ((Map)temp).get("modify");
							if(var != null && var instanceof Boolean)
							{
								datasetTemp._permissions.modify = ((Boolean)var).booleanValue();
							}

							var = ((Map)temp).get("manage");
							if(var != null && var instanceof Boolean)
							{
								datasetTemp._permissions.manage = ((Boolean)var).booleanValue();
							}

							var = ((Map)temp).get("view");
							if(var != null && var instanceof Boolean)
							{
								datasetTemp._permissions.view = ((Boolean)var).booleanValue();
							}
						}
						
						datasetList.add(datasetTemp);
						

				}else
				{
			       throw new IOException(String.format("Dataset  list download failed, invalid server response %s",dataset));
				}
			} //end for
		}else
		{
	       throw new IOException(String.format("Dataset list download failed, invalid server response %s",dataSetMap));
		}
		return datasetList;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<FolderType> listFolders(PartnerConnection partnerConnection) throws Exception {
		List<FolderType> folderList = new LinkedList<FolderType>();
		partnerConnection.getServerTimestamp();
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

		URI listEMURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/esObject/folder", null,null);			
		HttpGet listEMPost = new HttpGet(listEMURI);

		listEMPost.setConfig(requestConfig);
		listEMPost.addHeader("Authorization","OAuth "+sessionID);			
//		System.out.println("Fetching Folder list from server, this may take a minute...");
		CloseableHttpResponse emresponse = httpClient.execute(listEMPost);
		   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
	       int statusCode = emresponse.getStatusLine().getStatusCode();
	       if (statusCode != HttpStatus.SC_OK) {
		       throw new IOException(String.format("listFolders failed: %d %s", statusCode,reasonPhrase));
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
				List<Map> folders = (List<Map>) res.get("result");
				if(folders != null && !folders.isEmpty())
				{
					for(Map job:folders)
					{
						String _type = (String) job.get("_type");
						if(_type != null && _type.equals("folder"))
						{
								FolderType folder = new FolderType();
								
								Object temp = job.get("_createdDateTime");
								if(temp != null && temp instanceof Number)
								{
									folder._createdDateTime = ((Number)temp).longValue();
								}

								temp = job.get("lastModified");
								if(temp != null && temp instanceof Number)
								{
									folder.lastModified = ((Number)temp).longValue();
								}

								folder._type  = (String) job.get("_type");
								
								folder._uid  = (String) job.get("_uid");
								
								folder.assetIcon = (String) job.get("_type");
								
								folder.assetIconUrl = (String) job.get("assetIconUrl");
								
								folder.assetSharingUrl = (String) job.get("assetSharingUrl");
								
								folder.developerName = (String) job.get("developerName");

								folder.name = (String) job.get("name");
								
								folder._url = (String) job.get("_url");

								temp =  job.get("_permissions");
								if(temp != null && temp instanceof Map)
								{
									folder._permissions = folder.new PermissionType();
									Object var = ((Map)temp).get("modify");
									if(var != null && var instanceof Boolean)
									{
										folder._permissions.modify = ((Boolean)var).booleanValue();
									}

									var = ((Map)temp).get("manage");
									if(var != null && var instanceof Boolean)
									{
										folder._permissions.manage = ((Boolean)var).booleanValue();
									}

									var = ((Map)temp).get("view");
									if(var != null && var instanceof Boolean)
									{
										folder._permissions.view = ((Boolean)var).booleanValue();
									}
								}
								
								folderList.add(folder);
								

						}else
						{
					       throw new IOException(String.format("Dataflow Folder list download failed, invalid server response %s",emList));
						}
					} //end for
				}else
				{
			       throw new IOException(String.format("Dataflow Folder list download failed, invalid server response %s",emList));
				}
		}
		return folderList;
}

	public static List<Session> listSessions(PartnerConnection connection) throws Exception 
	{
		return Session.listSessions(connection.getUserInfo().getOrganizationId());
	}
	
	public static PartnerConnection login(int retryCount,String username,String password, String token, String endpoint, String sessionId, boolean debug) throws ConnectionException, MalformedURLException  {

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
			endpoint = DatasetUtilConstants.defaultEndpoint;
		}
		
		if(sessionId != null && !sessionId.isEmpty())
		{
			while(endpoint.toLowerCase().contains("login.salesforce.com") || endpoint.toLowerCase().contains("test.salesforce.com") || endpoint.toLowerCase().contains("test") || endpoint.toLowerCase().contains("prod") || endpoint.toLowerCase().contains("sandbox"))
			{
				throw new IllegalArgumentException("ERROR: endpoint must be the actual serviceURL and not the login url");
			}
		}

		URL uri = new URL(endpoint);
		String protocol = uri.getProtocol();
		String host = uri.getHost();
		if(protocol == null || !protocol.equalsIgnoreCase("https"))
		{
			if(host == null || !(host.toLowerCase().endsWith("internal.salesforce.com") || host.toLowerCase().endsWith("localhost")))
			{
				System.out.println("\nERROR: Invalid endpoint. UNSUPPORTED_CLIENT: HTTPS Required in endpoint");
				System.exit(-1);
			}
		}
		
		if(uri.getPath() == null || uri.getPath().isEmpty() || uri.getPath().equals("/"))
		{
			uri = new URL(uri.getProtocol(), uri.getHost(), uri.getPort(), DatasetUtilConstants.defaultSoapEndPointPath); 
		}
		endpoint = uri.toString();
		

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
			@SuppressWarnings("unused")
			GetUserInfoResult userInfo = connection.getUserInfo();
			if(!hasLoggedIn)
			{
				System.out.println("\nLogging in ...");
				System.out.println("Service Endpoint: " + config.getServiceEndpoint());
				if(debug)
					System.out.println("SessionId: " + config.getSessionId());
//				System.out.println("User id: " + userInfo.getUserName());
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
				return login(retryCount,username, password, null, endpoint, sessionId, debug);
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

