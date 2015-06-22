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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.flow.node.AugmentNode;
import com.sforce.dataset.flow.node.EdgemartNode;
import com.sforce.dataset.flow.node.RegisterNode;
import com.sforce.dataset.metadata.DatasetSchema;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class DatasetAugmenter {
	
//	String username = null;
//	String password = null;
//	String endpoint = null;
//	String token = null;
//	String sessionId = null;

/*
	public DatasetAugmenter(String username,String password, String token, String endpoint,String sessionId) throws Exception 
	{
		super();
		this.username = username;
		this.password = password;
		this.token = token;
		this.endpoint = endpoint;
		this.sessionId = sessionId;
	}
*/

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void augmentEM(PartnerConnection partnerConnection) throws Exception
	{
		Map<String, Map> map = DatasetUtils.listPublicDataset(partnerConnection, true);
		System.out.println("\n");
		if(map==null || map.size()==0)
		{
			System.out.println("No dataset found in org");
			return;
		}
		int cnt = 1;
		for(String alias:map.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+map.size()+"} Datasets...");
			System.out.println(cnt+". "+alias);
			cnt++;
		}
		System.out.println("\n");

		String leftDataSet = null;
		String rightDataSet = null;
		
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter left Dataset Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(String alias:map.keySet())
				{
					if(cnt==left)
					{
						leftDataSet = alias;
						break;
					}
					cnt++;
				}
				if(leftDataSet!=null)
					break;
			}catch(Throwable me)
			{				
				leftDataSet = null;
			}
		}

		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter Right Dataset Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(String alias:map.keySet())
				{
					if(cnt==left)
					{
						rightDataSet = alias;
						break;
					}
					cnt++;
				}
				if(rightDataSet!=null)
					break;
			}catch(Throwable me)
			{				
				rightDataSet = null;
			}
		}
		System.out.println("\n");
		System.out.println("Getting dimensions in dataset {"+leftDataSet+"}");
		downloadEMJson(map.get(leftDataSet), partnerConnection);
		if(!leftDataSet.equals(rightDataSet))
		{
			System.out.println("Getting dimensions in dataset {"+rightDataSet+"}");
			downloadEMJson(map.get(rightDataSet), partnerConnection);
		}
//		Map lmj = DatasetSchema.load(new File(leftDataSet));
		Map<String,Map> leftDims = DatasetSchema.getDimensions(new File(leftDataSet));
		System.out.println("\n");
		if(leftDims==null || leftDims.size()==0)
		{
			System.out.println("No Dimensions found in Datasets {"+leftDataSet+"}");
			return;
		}
		cnt = 1;
		for(String dim:leftDims.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+leftDims.size()+"} Dimensions in Dataset {"+leftDataSet+"}...");
			System.out.println(cnt+". "+dim);
			cnt++;
		}
		String leftKey = null;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("(Left Key) Enter Dimension Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(String dim:leftDims.keySet())
				{
					if(cnt==left)
					{
						leftKey = dim;
						break;
					}
					cnt++;
				}
				if(leftKey!=null)
					break;
			}catch(NumberFormatException me)
			{				
				leftKey = null;
			}
		}

		Map<String,Map> rightDims = DatasetSchema.getDimensions(new File(rightDataSet));
		System.out.println("\n");
		if(rightDims==null || rightDims.size()==0)
		{
			System.out.println("No Dimensions found in Datasets {"+rightDataSet+"}");
			return;
		}
		cnt = 1;
		for(String dim:rightDims.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+rightDims.size()+"} Dimensions in Dataset {"+rightDataSet+"}...");
			System.out.println(cnt+". "+dim);
			cnt++;
		}
		String rightKey = null;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("(Right Key) Enter Dimension Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(String dim:rightDims.keySet())
				{
					if(cnt==left)
					{
						rightKey = dim;
						break;
					}
					cnt++;
				}
				if(rightKey!=null)
					break;
			}catch(NumberFormatException me)
			{				
				rightKey = null;
			}
		}

		Map<String,Map> rightFields = DatasetSchema.getFields(new File(rightDataSet));		
		for(String dim:rightFields.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+rightDims.size()+"} Fields in Dataset {"+rightDataSet+"}...");
			System.out.println(cnt+". "+dim);
			cnt++;
		}

		LinkedList<String> rightKeys = new LinkedList<String>();
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter the Field to select from right Dataset (Comma seperated numbers): ");
				String[] tmpArray = tmp.trim().split(",");
				for(String tmp2:tmpArray)
				{
					String tempKey = null;
					long left = Long.parseLong(tmp2);
					cnt = 1;
					for(String dim:rightFields.keySet())
					{
						if(cnt==left)
						{
							tempKey = dim;
							break;
						}
						cnt++;
					}
					if(tempKey==null)
					{
						rightKeys.clear();
						break;
					}
					rightKeys.add(tempKey);
				}
			}catch(Throwable t)
			{				
				rightKeys.clear();
			}
			if(!rightKeys.isEmpty())
				break;
		}

		String newDataSetName = null;
		System.out.println("\n");
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter New Dataset Name: ");
				if(tmp!=null && !tmp.trim().isEmpty() && !tmp.trim().equalsIgnoreCase(leftDataSet) && !tmp.trim().equalsIgnoreCase(rightDataSet) && !tmp.trim().endsWith("__c"))
					newDataSetName = tmp.trim();
				if(newDataSetName!=null)
					break;
			}catch(Exception me)
			{				
				newDataSetName = null;
			}
		}
		
		String newDataSetAlias = DatasetUtils.replaceSpecialCharacters(newDataSetName);

//		String joinCondition = leftDataSet+"."+leftKey+"="+rightDataSet+"."+rightKey;
//		System.out.println("\n");
//		System.out.println("Join Condition: "+ joinCondition);
		String workflowName = "SalesEdgeEltWorkflow";
		File dataflowFile = new File(workflowName+".json");

		LinkedHashMap wfdef = new LinkedHashMap();
		wfdef.put(leftDataSet, EdgemartNode.getNode(leftDataSet));
		wfdef.put(rightDataSet, EdgemartNode.getNode(rightDataSet));
		wfdef.put(leftDataSet+"2"+rightDataSet,AugmentNode.getNode(leftDataSet, rightDataSet, leftKey, rightKey, rightKeys.toArray(new String[0])));
		wfdef.put("Register"+leftDataSet+"2"+rightDataSet,RegisterNode.getNode(leftDataSet+"2"+rightDataSet, newDataSetAlias, newDataSetName, null));
		System.out.println("\n");
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(wfdef));
			mapper.writerWithDefaultPrettyPrinter().writeValue(dataflowFile, wfdef);
			System.out.println("\ndataflow succesfully generated and saved in file {"+dataflowFile+"}");
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {		 
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("\n");

		boolean uploadAndStart = false;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Upload and start new workflow? (Yes/No): ");
				if(tmp!=null && (tmp.equalsIgnoreCase("yes") || tmp.equalsIgnoreCase("y")))
				{
					uploadAndStart = true;
					break;
				}else if(tmp!=null && (tmp.equalsIgnoreCase("no") || tmp.equalsIgnoreCase("n")))
				{
					uploadAndStart = false;
					break;
				}
			}catch(Throwable me)
			{				
				uploadAndStart = false;
			}
		}
		
		if(uploadAndStart)
		{
			DataFlowUtil.uploadAndStartDataFlow(partnerConnection, wfdef, workflowName);
		}
		
	}

	@SuppressWarnings({ "rawtypes" })
	public static void downloadEMJson(Map resp, PartnerConnection partnerConnection) throws URISyntaxException, IOException, ConnectionException
	{
		String _alias = null;
		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();

		String serviceEndPoint = config.getServiceEndpoint();

		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		   
		URI u = new URI(serviceEndPoint);

		
	if(resp!=null)
	{
		_alias = (String) resp.get("_alias");
		Integer _createdDateTime = (Integer) resp.get("_createdDateTime");
		if(_createdDateTime != null)
		{
		}
		Map edgemartData = (Map) resp.get("edgemartData");
		if(edgemartData != null)
		{
		}
		Map _files = (Map) resp.get("_files");
		if(_files != null)
		{
			File edgemartDir = new File(_alias);
			FileUtils.forceMkdir(edgemartDir);
			for(Object filename:_files.keySet())
			{
				
				if(!filename.toString().endsWith("json"))
					continue;
				
				CloseableHttpClient httpClient1 = HttpUtils.getHttpClient();

				String url = (String) _files.get(filename);
				URI listEMURI1 = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), url, null,null);			
				HttpGet listEMPost1 = new HttpGet(listEMURI1);

//				System.out.println("Downloading file {"+filename+"} from url {"+listEMURI1+"}");
				listEMPost1.setConfig(requestConfig);
				listEMPost1.addHeader("Authorization","OAuth "+sessionID);			

				CloseableHttpResponse emresponse1 = httpClient1.execute(listEMPost1);

			   String reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
		       int statusCode = emresponse1.getStatusLine().getStatusCode();
		       if (statusCode != HttpStatus.SC_OK) {
		           System.out.println("Method failed: " + reasonPhrase);
		           continue;
		       }
//		       System.out.println(String.format("statusCode: %d", statusCode));
//		       System.out.println(String.format("reasonPhrase: %s", reasonPhrase));

				HttpEntity emresponseEntity1 = emresponse1.getEntity();
				InputStream emis1 = emresponseEntity1.getContent();
				File outfile = new File(edgemartDir,(String) filename);
//				System.out.println("file {"+outfile+"}. Content-length {"+emresponseEntity1.getContentLength()+"}");
				BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outfile));
				IOUtils.copy(emis1, out);
				out.close();
				emis1.close();
				httpClient1.close();
//				System.out.println("file {"+outfile+"} downloaded. Size{"+outfile.length()+"}");
			}
		}
//		System.out.println("\n Completed downloading EM Files..");							
	}
	}
	
}
