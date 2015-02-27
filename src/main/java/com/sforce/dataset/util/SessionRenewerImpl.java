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

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SessionHeader_element;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

    public class SessionRenewerImpl implements com.sforce.ws.SessionRenewer {
    	
        public static final javax.xml.namespace.QName SESSION_HEADER_QNAME =
                new javax.xml.namespace.QName("urn:partner.soap.sforce.com", "SessionHeader");

        String username = null;
		String password = null;
		String endpoint = null;
		String token = null; 

		public SessionRenewerImpl(final String username,
				String password, String token, String endpoint) {
			super();
			this.username = username;
			this.password = password;
			this.token = token;
			this.endpoint = endpoint;
		}

        @Override
        public SessionRenewalHeader renewSession(ConnectorConfig config) throws ConnectionException {
//            PartnerConnection connection = Connector.newConnection(config);
        	
        	config.setSessionId(null);
            
//			ConnectorConfig config1 = new ConnectorConfig();
//			config1.setUsername(username);
//			config1.setPassword(password);
//			config1.setAuthEndpoint(endpoint);
//			config1.setSessionRenewer(new SessionRenewerImpl(username, password, token, endpoint));
//			PartnerConnection connection = new PartnerConnection(config1);
			PartnerConnection connection = Connector.newConnection(config);
			
			connection.getUserInfo();

//			config.setSessionId(connection.getConfig().getSessionId());
//			config.setServiceEndpoint(connection.getConfig().getServiceEndpoint());

            SessionRenewalHeader header = new SessionRenewalHeader();
            header.name = SESSION_HEADER_QNAME;
//            header.name = new QName("urn:partner.soap.sforce.com", "SessionHeader");
            SessionHeader_element se = new SessionHeader_element();
            se.setSessionId(config.getSessionId());
            header.headerElement = se;
            
            return header;
        }

    }