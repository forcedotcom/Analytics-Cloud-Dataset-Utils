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
package com.sforce.dataset.connector.sfdc;

import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sforce.dataset.connector.ConnectorUtils;
import com.sforce.dataset.connector.IConnector;
import com.sforce.dataset.connector.InputPipeline;
import com.sforce.dataset.connector.OutputPipeline;
import com.sforce.dataset.connector.WriteOperation;
import com.sforce.dataset.connector.annotation.ConnectionProperty;
import com.sforce.dataset.connector.exception.ConnectionException;
import com.sforce.dataset.connector.exception.DataConversionException;
import com.sforce.dataset.connector.exception.DataReadException;
import com.sforce.dataset.connector.exception.DataWriteException;
import com.sforce.dataset.connector.exception.FatalException;
import com.sforce.dataset.connector.exception.MetadataException;
import com.sforce.dataset.connector.metadata.ConnectionPropertyType;
import com.sforce.dataset.connector.metadata.FieldType;
import com.sforce.dataset.connector.metadata.ObjectType;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.soap.partner.PartnerConnection;

public class SfdcConnector implements IConnector {

			private static final Log logger = LogFactory.getLog(SfdcConnector.class);
			public static final String PROD_SERVER = "https://login.salesforce.com";
		    	    
			@ConnectionProperty(label = "Username", type = ConnectionPropertyType.STRING, required = true)
		    public String username = null;    
		    
			@ConnectionProperty(label = "Password", type = ConnectionPropertyType.STRING, required = true, password = true)
			public String password = null;

			@ConnectionProperty(label = "Server URL", type = ConnectionPropertyType.STRING, required = true)
			public String connectionUrl = PROD_SERVER;

			@ConnectionProperty(label = "Security Token", type = ConnectionPropertyType.STRING, password = true)
			public String token = null;
					
		public String getUsername() {
			return username;
		}

		public void setUsername(String username) {
			this.username = username;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}

		public String getToken() {
			return token;
		}

		public void setToken(String token) {
			this.token = token;
		}

		public String getConnectionUrl() {
			return connectionUrl;
		}

		public void setConnectionUrl(String connectionUrl) {
			this.connectionUrl = connectionUrl;
		}

		private PartnerConnection partnerConnection = null;
					

		public SfdcConnector() {
			try
			{
				//logger.debug("java.class.path: " +System.getProperty("java.class.path")); 						
				//logger.debug("ContextClassLoader Path: " +ConnectorUtils.getClassLoaderClassPath((URLClassLoader) Thread.currentThread().getContextClassLoader())); 	        
				logger.debug("SFDCConnectorImpl ClassLoader Path: " +ConnectorUtils.getClassLoaderClassPath((URLClassLoader) SfdcConnector.class.getClassLoader()));			
			}catch(Throwable t){t.printStackTrace();}
		}
		
	//Begin Connector Implementation
		
		@Override
		public UUID getConnectorID() {
			return UUID.fromString(SFDCConnectorConstants.PLUGIN_UUID);
		}


		@Override
		public void connect() throws ConnectionException
		{
			logger.debug("SFDCConnectorImpl.connect()");
			try {
				partnerConnection = DatasetUtils.login(0, username, password, token, connectionUrl, connectionUrl, false);
			} catch (MalformedURLException e) {
				throw new ConnectionException(e.getMessage());
			} catch (com.sforce.ws.ConnectionException e) {
				throw new ConnectionException(e.getMessage());
			}
		}
		
		@Override
		public void disconnect() throws ConnectionException {
			logger.debug("SFDCConnectorImpl.disconnect()");
			partnerConnection = null;
		}
		

		@Override
		public List<ObjectType> getObjectList(boolean isSource) throws MetadataException {
			return SFDCUtils.getObjectList(partnerConnection,Pattern.compile(".*"),isSource);
		}

		@Override
		public List<FieldType> getFieldList(ObjectType object,boolean isSource) throws MetadataException {
			return SFDCUtils.getFieldList(object, partnerConnection, isSource);
		}

		@Override
		public void read(OutputPipeline buffer, ObjectType object,List<FieldType> fields, int batchSize) throws ConnectionException, DataReadException, DataConversionException, FatalException {
			SFDCUtils.read(partnerConnection, buffer, object, fields,batchSize, null);
			return;
		}

		@Override
		public void write(InputPipeline buffer, WriteOperation operation,
				ObjectType object, List<FieldType> fields) throws ConnectionException, DataWriteException, DataConversionException, FatalException {
			SFDCUtils.write(partnerConnection, buffer, fields, object, operation) ;
			return;
		}

		@Override
		public String getConnectorName() {
			return "Salesforce";
		}

		@Override
		public String getConnectorType() {
			return "Salesforce";
		}	


	}

