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
package com.sforce.dataset.server;

import java.awt.Desktop;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.net.URL;

import javax.swing.JFrame;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;


public class DatasetUtilServer {
    
    static private final String DEFAULT_HOST = "127.0.0.1";
    static private final int DEFAULT_PORT = 3174;
        
    static private int port = DEFAULT_PORT;
    static private String host = DEFAULT_HOST;
    
//    static PartnerConnection partnerConnection;

        
    public static void main(String[] args) throws Exception {
	    DatasetUtilServer datasetUtilServer = new DatasetUtilServer();
	    datasetUtilServer.init(args, true);
    }

    public void init(String[] args, boolean join) throws Exception {

//    	DatasetUtilServer.partnerConnection = partnerConnection;
        final String WEBAPPDIR = "login.html";
        final String contextPath = "/";
        final int maxFormContentSize = 40 * 1000 * 1024 * 1024;

        final Server server = new Server();
        
        ServerConnector connector = new ServerConnector(server);
        connector.setHost(host);
        connector.setPort(port);
        server.addConnector(connector);
        
        final URL warUrl = this.getClass().getClassLoader().getResource(WEBAPPDIR);
        String warUrlString = "src/main/webapp";
        System.out.println("warUrl:"+warUrlString);
        if(warUrl!=null)
        {
        	warUrlString = warUrl.toExternalForm();
            System.out.println("warUrlString:"+warUrlString);
        	warUrlString = warUrlString.replace(WEBAPPDIR, "");
        }
        System.out.println("warUrlString:"+warUrlString);
        WebAppContext context = new WebAppContext(warUrlString, contextPath);
        context.setMaxFormContentSize(maxFormContentSize);
        server.setHandler(context);
        
        server.setStopAtShutdown(true);
//      this.setSendServerVersion(true);

      // start the server
      try {
      	server.start();
      } catch (BindException e) {
 		System.out.println("\n\t\t**************************************************");
 		System.out.println("Cannot start Datsetutils,maybe datasetutils is already running"); 
 		System.out.println("\t\t**************************************************\n");			
      	//e.printStackTrace();
          throw e;
      }
      
        
        boolean headless = false;
        if (headless) {
            System.setProperty("java.awt.headless", "true");
        } else {
            try {
            	DatasetUtilClient client = new DatasetUtilClient();
                client.init(host,port);
            } catch (Exception e) {
                System.err.println("Sorry, some error prevented us from launching the browser for you.\n\n Point your browser to http://" + host + ":" + port + "/ to start using DatasetUtilServer.");
            }
        }
        
        // hook up the signal handlers
        Runtime.getRuntime().addShutdownHook(
            new Thread(new ShutdownSignalHandler(server))
        );
 
        if(join)
        	server.join();
    }
}


class DatasetUtilClient extends JFrame implements ActionListener {
    
    private static final long serialVersionUID = 7886547342175227132L;

    private URI uri;
    
    public void init(String host, int port) throws Exception {

        uri = new URI("http://" + "127.0.0.1" + ":" + port + "/");

        openBrowser();
    }
    
    @Override
    public void actionPerformed(ActionEvent e) { 
        String item = e.getActionCommand(); 
        if (item.startsWith("Open")) {
            openBrowser();
        }
    } 
    
    private void openBrowser() {
        if (!Desktop.isDesktopSupported()) {
            System.err.println("Java Desktop class not supported on this platform.  Please open %s in your browser: "+uri.toString());
        }
        try {
            Desktop.getDesktop().browse(uri);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class ShutdownSignalHandler implements Runnable {
    
    private Server _server;

    public ShutdownSignalHandler(Server server) {
        this._server = server;
    }

    @Override
    public void run() {

        _server.setStopTimeout(3000);

        try {
            _server.stop();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
    
