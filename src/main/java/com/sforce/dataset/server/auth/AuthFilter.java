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

package com.sforce.dataset.server.auth;

import java.io.IOException;
import java.security.Principal;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import com.sforce.soap.partner.GetServerTimestampResult;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;

public class AuthFilter implements Filter {

    static final String FILTER_ALREADY_VISITED = "__force_auth_filter_already_visited";
    static final String SECURITY_AUTH_SUBJECT = "javax.security.auth.subject";
    static final String SECURITY_CONFIG_NAME = "ForceLogin";
    static final String DEFAULT_USER_PROFILE = "myProfile";
    static final String CONTEXT_STORE_SESSION_VALUE = "session";
   
    public static final String FORCE_FORCE_SESSION = "force_sid";
    public static final String FORCE_FORCE_ENDPOINT = "force_ep";
    public static final String DEFAULT_ROLE = "ROLE_USER";
    public static final String REDIRECT_AUTH_URI = "/_auth";

    
 
    
    //logout specific parameters
    private String logoutUrl = "";
    private String loginUrl = "/login.html";
    private SecurityContextSessionStore securityContextSessionStore = null;
    
    /**
     * Initializes the filter from the init params.
     * {@inheritDoc} 
     */
    @Override
    public void init(FilterConfig config) throws ServletException {
            logoutUrl = "/logout";
            securityContextSessionStore = new SecurityContextSessionStore();
    }

    /**
     * Handle the secured requests.
     * {@inheritDoc} 
     */
    @Override
    public void doFilter(ServletRequest sreq, ServletResponse sres, FilterChain chain) throws IOException,
            ServletException {
        HttpServletRequest request = (HttpServletRequest) sreq;
        HttpServletResponse response = (HttpServletResponse) sres;


		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate,max-age=0");
		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");

		response.setHeader("Expires", "Tue, 01 Jan 1980 1:00:00 GMT");


        
        @SuppressWarnings("unused")
		String path = request.getServletPath();
        SecurityContext sc = null;
        
        if (request.getAttribute(FILTER_ALREADY_VISITED) != null) {
            // ensure we do not get into infinite loop here
            chain.doFilter(request, response);
            return;
        }
        
        if("/".equals(request.getServletPath()))
        {
            chain.doFilter(request, response);
            return;
        }

		if (isLogoutUrl(request)) {
			chain.doFilter(request, response);
			return;
		}
      
		if (isLoginUrl(request)) {
			chain.doFilter(request, response);
			return;
		}
        
		// if this isn't the callback from an OAuth handshake
        // get the security context from the session
        if (!REDIRECT_AUTH_URI.equals(request.getServletPath())) {
            sc  = securityContextSessionStore.retreiveSecurityContext(request);
        }
        
		if (sc != null) {
            PartnerConnection conn = getConnection(request);
			if (conn == null) {
				securityContextSessionStore.clearSecurityContext(request);
				sc = null;
			} 
		}

        // if there is no valid security context then initiate an OAuth handshake
		if (sc == null) {
			doLogin(request, response);
			return;
		}
        
        
        try {
            request.setAttribute(FILTER_ALREADY_VISITED, Boolean.TRUE);
            chain.doFilter(new AuthenticatedRequestWrapper(request, sc), response);
        } catch (SessionExpirationException e) {
            doLogin(request, response);
        } catch (SecurityException se) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, request.getRequestURI());
        } finally {
            try {
                request.removeAttribute(FILTER_ALREADY_VISITED);
            } finally {
            }
        }
    }

    /**
     * Sends the authentication redirect or saves the security context to the session depending
     * on which phase of the handshake we're in.
     * 
     * @param request
     * @param response
     * @throws IOException
     * @throws ServletException 
     */
    private void doLogin(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        response.addHeader("REQUIRES_AUTH","1");
        response.sendRedirect("/login.html");
    }

    /**
     * No resources to release.
     */
    @Override
    public void destroy() {  }


    /**
     * Wraps the request and provides methods to make the authenticated user information available.
     */
    private static final class AuthenticatedRequestWrapper extends HttpServletRequestWrapper {

        private final ForceUserPrincipal userP;
        private final ForceRolePrincipal roleP;

        public AuthenticatedRequestWrapper(HttpServletRequest request, SecurityContext sc) {
            super(request);
            this.userP = new ForceUserPrincipal(sc.getUserName(), sc.getSessionId());
            this.roleP = new ForceRolePrincipal(sc.getRole());
        }

        @Override
        public String getRemoteUser() {
            return userP != null ? userP.getName() : super.getRemoteUser();
        }

        @Override
        public Principal getUserPrincipal() {
            return userP != null ? userP : super.getUserPrincipal();
        }

        @Override
        public boolean isUserInRole(String role) {
            return roleP != null ? roleP.getName().endsWith(role) : super.isUserInRole(role);
        }
    }
    
    private boolean isLogoutUrl(HttpServletRequest request) {        
        if (logoutUrl != null
                && !"".equals(logoutUrl)
                && logoutUrl.equals(request.getServletPath())) {
            return true;
        }
        return false;
    }

    private boolean isLoginUrl(HttpServletRequest request) {        
        if (loginUrl != null
                && !"".equals(loginUrl)
                && loginUrl.equals(request.getServletPath())) {
            return true;
        }
        return false;
    }

    
//    public static SecurityContext verifyAndRefreshSecurityContext(SecurityContext sc, HttpServletRequest request) {
//        
//        //populate the security context with user information
//        if (sc.getSessionId() != null && sc.getEndPoint() != null) {
//            //attempt to connect to the partner API and retrieve the user data
//            //if this fails, set the security context to null because we'll
//            //need to redo the oauth handshake.
//            try {
//                sc = retrieveUserData(sc.getSessionId(), sc.getEndPoint());
//            } catch (Throwable t) {
////            	System.out.println("Force.com session is invalid. Refreshing... ");
//                sc = null;
//            }
//        }else
//        {
//        	sc = null;
//        }
//        
//        return sc;
//    }
    
    
//    public static SecurityContext retrieveUserData(String sessionId, String endpoint) throws ConnectionException {
//
//        if (sessionId == null) {
//            throw new IllegalArgumentException("session id must not be null");
//        }
//        if (endpoint == null) {
//            throw new IllegalArgumentException("endpoint must not be null");
//        }
//        
//        SecurityContext sc = new SecurityContext();
//        sc.setSessionId(sessionId);
//        sc.setEndPoint(endpoint);
//        if (endpoint.indexOf("/services/Soap/u") > 0) {
//            sc.setEndPointHost(endpoint.substring(0, endpoint.indexOf("/services/Soap/u")));
//        }
//
//        ConnectorConfig config = new ConnectorConfig();
//        config.setServiceEndpoint(sc.getEndPoint());
//        config.setSessionId(sc.getSessionId());
//        
//        GetUserInfoResult userInfoResult = Connector.newConnection(config).getUserInfo();
//        
//        sc.init(userInfoResult);
//
//        SObject[] results;
//        try {
//            results = Connector.newConnection(config).retrieve("Name", "Profile",
//                new String[] {userInfoResult.getProfileId()});
//        } catch (ConnectionException e) {
//            results = null;
//        }
//        
//        String role = null;
//        if (results != null && results.length > 0) {
//            SObject result = results[0];
//            role = (String) result.getField("Name");
//            
//            if (role.isEmpty()) {
//                role = DEFAULT_ROLE;
//            }
//        } else {
//            role = DEFAULT_ROLE;
//        }
//        
//        sc.setRole(role);
//
//        return sc;
//    }
    
    
    public static PartnerConnection getConnection(HttpServletRequest request) {
    	PartnerConnection conn = null;
		SecurityContextSessionStore securityContextSessionStore = new SecurityContextSessionStore();
        try {
			SecurityContext sc = securityContextSessionStore.retreiveSecurityContext(request);
			if (sc != null) 
			{
				if (sc.getConnection()!= null && sc.getSessionId() != null && sc.getEndPoint() != null && sc.getLastRefreshTimestamp() != null) 
				{
					conn = sc.getConnection();
//					if(sc.getLastRefreshTimestamp().getTimeInMillis() < (System.currentTimeMillis() - 60*000))
//					{
//						if (conn != null) {
//							GetUserInfoResult userInfoResult = conn.getUserInfo();
//							if(userInfoResult == null)
//							{
//								conn = null;
//							}else
//							{
//								GetServerTimestampResult serverTimestampResult = conn.getServerTimestamp();							
//								if (serverTimestampResult.getTimestamp() != null) {
//									sc.setLastRefreshTimestamp(serverTimestampResult.getTimestamp());
//								}else
//								{
//									conn = null;
//								}
//							}
//						}
//					}
				}
			} else {
				conn = null;
			}
        } catch (Throwable t) {
        	conn = null;
        }
        if(conn==null)
        {
			securityContextSessionStore.clearSecurityContext(request);
        }
		return conn;
    }    


}
