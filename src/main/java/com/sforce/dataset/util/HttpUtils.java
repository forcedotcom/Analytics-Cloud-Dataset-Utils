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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;

import com.sforce.dataset.DatasetUtilConstants;

public class HttpUtils {
	
	public static CloseableHttpClient getHttpClient() throws UnknownHostException
	{
		return getHttpClient(DatasetUtilConstants.getSystemConfig());
	}
	
	public static CloseableHttpClient getHttpClient(com.sforce.dataset.Config conf) throws UnknownHostException
	{
        if (conf.proxyHost != null && conf.proxyHost.length() > 0 && conf.proxyPort > 0) 
        {
            String proxyUser = conf.proxyUsername == null ? "" : conf.proxyUsername;
            String proxyPassword = conf.proxyPassword == null ? "" : conf.proxyPassword;
 
        	Credentials credentials;
			 
			 if (conf.proxyNtlmDomain != null && !conf.proxyNtlmDomain.trim().isEmpty()) 
			 {
				 String computerName = InetAddress.getLocalHost().getCanonicalHostName();
				 credentials = new NTCredentials(proxyUser, proxyPassword,computerName, conf.proxyNtlmDomain);
			 }else
			 {
	             credentials = new UsernamePasswordCredentials(conf.proxyUsername, conf.proxyPassword);
			 }
			 
			CredentialsProvider credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials( new AuthScope(conf.proxyHost,conf.proxyPort), credentials);
			HttpClientBuilder clientBuilder = HttpClientBuilder.create();
		
			clientBuilder.useSystemProperties();
			clientBuilder.setProxy(new HttpHost(conf.proxyHost,conf.proxyPort));
			clientBuilder.setDefaultCredentialsProvider(credsProvider);
			clientBuilder.setProxyAuthenticationStrategy(new ProxyAuthenticationStrategy());
		
			CloseableHttpClient httpClient = clientBuilder.build();
			return httpClient;
        }else
        {
    		CloseableHttpClient httpClient = HttpClients.createDefault();
			return httpClient;
        }
        
	}
	
	public static RequestConfig getRequestConfig()
	{
		RequestConfig requestConfig = RequestConfig.custom()
			       .setSocketTimeout(60000)
			       .setConnectTimeout(60000)
			       .setConnectionRequestTimeout(60000)
			       .build();
		return requestConfig;
	}
	
    public static Map<String, String> getQueryParameters(String url) throws URISyntaxException {
        url = url.replace("#","?");
        Map<String, String> params = new HashMap<>();
        List<NameValuePair> kvp = new URIBuilder(url).getQueryParams();
		for(NameValuePair j:kvp)
		{
			 params.put(j.getName(), j.getValue());			
		}
        return params;
    }
    
    public static void testProxyConfig(com.sforce.dataset.Config conf) throws URISyntaxException, ClientProtocolException, IOException
    {
    	CloseableHttpClient client = getHttpClient(conf);
		RequestConfig requestConfig = HttpUtils.getRequestConfig();
		   
		URI u = new URI("https://login.salesforce.com/");
		
		HttpGet testGet = new HttpGet(u);
	
		testGet.setConfig(requestConfig);
	
		CloseableHttpResponse emresponse1 = client.execute(testGet);
	
		String reasonPhrase = emresponse1.getStatusLine().getReasonPhrase();
		int statusCode = emresponse1.getStatusLine().getStatusCode();
		if (statusCode != HttpStatus.SC_OK) {
			throw new IOException(String.format("ProxyConfig Test failed: %d %s",statusCode,reasonPhrase));
		}
	
		HttpEntity emresponseEntity1 = emresponse1.getEntity();
		InputStream emis1 = emresponseEntity1.getContent();
		String loginPage = IOUtils.toString(emis1, "UTF-8");
		//TODO Check if its salesforce login page or proxy login page
		emis1.close();
		client.close();

    	
    }

}
