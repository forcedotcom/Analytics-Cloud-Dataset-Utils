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
package com.sforce.dataset.saql;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.metadata.DatasetXmd;
import com.sforce.dataset.util.HttpUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SaqlUtil {

	@SuppressWarnings("unchecked")
	public static List<Map<String,Object>> queryDataset(PartnerConnection partnerConnection, String saqlQuery) throws ConnectionException, IllegalStateException, IOException, URISyntaxException
	{
		List<Map<String,Object>> records = null;
//		partnerConnection.getServerTimestamp();
		ConnectorConfig config = partnerConnection.getConfig();			
		String sessionID = config.getSessionId();
		String serviceEndPoint = config.getServiceEndpoint();

		CloseableHttpClient httpClient = HttpUtils.getHttpClient();
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		
		URI u = new URI(serviceEndPoint);

		URI patchURI = new URI(u.getScheme(),u.getUserInfo(), u.getHost(), u.getPort(), "/insights/internal_api/v1.0/remote", null,null);			
		
        HttpPost httpPatch = new HttpPost(patchURI);
        
        SaqlRequest req = new SaqlRequest();
		req.query = saqlQuery;
		
		ObjectMapper mapper = new ObjectMapper();			
        StringEntity entity = new StringEntity(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(req), "UTF-8");        
        entity.setContentType("application/json");
        httpPatch.setConfig(requestConfig);
        httpPatch.setEntity(entity);
        httpPatch.addHeader("Authorization","OAuth "+sessionID);			
		CloseableHttpResponse emresponse = httpClient.execute(httpPatch);
	   String reasonPhrase = emresponse.getStatusLine().getReasonPhrase();
       int statusCode = emresponse.getStatusLine().getStatusCode();
		HttpEntity emresponseEntity = emresponse.getEntity();
		InputStream emis = emresponseEntity.getContent();			
		String response = IOUtils.toString(emis, "UTF-8");
		emis.close();
		httpClient.close();

		if (statusCode != HttpStatus.SC_OK) {
			if(response!=null && !response.trim().isEmpty())
				throw new IOException(String.format("Saql failed: %s", response));
			else
		       throw new IOException(String.format("Saql failed: %d %s ", statusCode,reasonPhrase));
	       }

		if(response!=null && !response.isEmpty())
		{
				mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				@SuppressWarnings("rawtypes")
				Map res =  mapper.readValue(response, Map.class);
//				mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, res);
				@SuppressWarnings({ "rawtypes" })
				List result = (List) res.get("result");
				if(result != null)
				{
					for(Object r:result)
					{
						if(r!=null)
						{
							Map<String, ?> res2 =  mapper.readValue(r.toString(), Map.class);
							if(res2!=null)
							{
								Map<?, ?> results2 = (Map<String, ?>) res2.get("results");
								if(results2!=null)
								{
									records = (List<Map<String, Object>>) results2.get("records");
								}
							}
						}
					}
				}
			//System.out.println(emList);
		}
		return records;
	}
	
	public static String generateSaql(PartnerConnection partnerConnection,String datasetId,String datasetVersion, DatasetXmd datasetXmd)
	{
		String saqlQuery = "q = load \"%s/%s\"; q = foreach q generate %s; q = limit q 100;";
		String columnString = "'%s' as '%s'";
		@SuppressWarnings("unchecked")
		Map<String,String> dims = (Map<String,String>) datasetXmd.labels.get("dimensions");
		@SuppressWarnings("unchecked")
		Map<String, String> measures = (Map<String,String>) datasetXmd.labels.get("measures");
		StringBuffer q = new StringBuffer();
		for(String dim:dims.keySet())
		{
			if(dim.endsWith("_Day") || dim.endsWith("_Month") || dim.endsWith("_Year") || dim.endsWith("_Quarter") || dim.endsWith("_Week") || dim.endsWith("_Hour")|| dim.endsWith("_Minute")|| dim.endsWith("_Second")|| dim.endsWith("_Month_Fiscal")|| dim.endsWith("_Year_Fiscal")|| dim.endsWith("_Quarter_Fiscal") || dim.endsWith("_Week_Fiscal"))
			{
				continue;
			}
			if(q.length()!=0)
			{
				q.append(", ");
			}
			q.append(String.format(columnString, dim,dim));
		}
		for(String meas:measures.keySet())
		{
			if(meas.endsWith("_sec_epoch") || meas.endsWith("_day_epoch"))
			{
				continue;
			}
			if(q.length()!=0)
			{
				q.append(", ");
			}
			q.append(String.format(columnString, meas,meas));
		}
		return String.format(saqlQuery,datasetId,datasetVersion,q.toString());

	}
	
}
