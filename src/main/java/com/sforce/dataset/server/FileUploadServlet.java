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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

@MultipartConfig 
public class FileUploadServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	private static final ThreadPoolExecutor executorPool = new ThreadPoolExecutor(2, 4, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2), Executors.defaultThreadFactory());
	private static List<FileUploadRequest> files = new LinkedList<FileUploadRequest>();

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try
		{
//			String pathInfo = request.getPathInfo();
//			if(pathInfo.equalsIgnoreCase("upload"))
//			{
					files.clear();
					files.addAll(MultipartRequestHandler.uploadByApacheFileUpload(request));
					CsvUploadWorker worker = new CsvUploadWorker(files, DatasetUtilServer.partnerConnection);
				    executorPool.execute(worker);
				    int cnt=0;
				    while(cnt<10)
				    {
				    	if(worker.isDone())
				    	{
				    		if(!worker.isUploadStatus())
				    		{
				    			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "File upload failed check {"+worker.getLogFile()+"} for details");
				    			return;
				    		}
				    		break;
				    	}
				    	try
				    	{
				    		Thread.sleep(3000);
				    	}catch(Throwable t)
				    	{
				    		break;
				    	}
				    	cnt++;
				    }
				    
				    
				    response.setContentType("application/json");
			    	ObjectMapper mapper = new ObjectMapper();
			    	mapper.writeValue(response.getOutputStream(), files);
//			}else
//			{
//				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+request.getPathInfo()+"}");
//			}
		}catch(Throwable t)
		{
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+t.toString()+"}");
		}
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		try 
		{		
//			String pathInfo = request.getPathInfo();
//			if(pathInfo.equalsIgnoreCase("upload"))
//			{
					String value = request.getParameter("f");
					FileUploadRequest getFile = files.get(Integer.parseInt(value));
					response.setContentType(getFile.getInputFileType());
				 	
				 	response.setHeader("Content-disposition", "attachment; filename=\""+getFile.getInputFileName()+"\"");
				 	
			        InputStream input = new FileInputStream(getFile.savedFile);
			        OutputStream output = response.getOutputStream();
			        IOUtils.copy(input, output);
			        output.close();
			        input.close();
//			}else
//			{
//				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+request.getPathInfo()+"}");
//			}
		 }catch (Throwable t) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Request {"+t.toString()+"}");
		 }
	}
}
