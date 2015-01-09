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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.loader.file.schema.ExternalFileSchema;

public class MultipartRequestHandler {

	public static File datadir = new File(DatasetUtilConstants.currentDir,"Data");
	
	public static List<FileUploadRequest> uploadByApacheFileUpload(HttpServletRequest request) throws IOException, ServletException, FileUploadException{
				
		List<FileUploadRequest> files = new LinkedList<FileUploadRequest>();
		
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);
		FileUploadRequest temp = null;
		
		if(isMultipart){

			DiskFileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload upload = new ServletFileUpload(factory);
			
				List<FileItem> items = upload.parseRequest(request);
				String datasetName = null;
				String datasetLabel = null;
				String datasetApp = null;
				String operation = null;
				String inputCsv = null;
				String inputJson = null;
				
				for(FileItem item:items){
				    if (item.isFormField()) {
				        if(item.getFieldName().equals("DatasetName"))
				        {
				        	datasetName = item.getString();
				    		if(datasetName!=null && !datasetName.isEmpty())
				    		{
				    			datasetLabel = datasetName;
				    			datasetName = ExternalFileSchema.createDevName(datasetName, "Dataset", 1);
				    		}
				        }
				        if(item.getFieldName().equals("DatasetApp"))
				        	datasetApp = item.getString();
				        if(item.getFieldName().equals("Operation"))
				        	operation = item.getString();
				        if(item.getFieldName().equals("inputCsv"))
				        	inputCsv = item.getString();
				        if(item.getFieldName().equals("inputJson"))
				        	inputJson = item.getString();
				    } else {
				    	if(item.getSize()>0 && item.getInputStream() != null)
				    	{
				    		temp = new FileUploadRequest();
							temp.setInputFileName(item.getName());
							temp.inputFileStream = item.getInputStream();
							temp.setInputFileSize(item.getSize()+"");
							files.add(temp);
				    	}
				       
				    }
				}

				File parent = new File(datadir,datasetName);
				FileUtils.forceMkdir(parent);
				
				for(FileUploadRequest fm:files){
					fm.setDatasetName(datasetName);
					fm.setDatasetLabel(datasetLabel);
					fm.setDatasetApp(datasetApp);
					fm.setOperation(operation);
					fm.setInputCsv(inputCsv);
					fm.setInputJson(inputJson);
					File outFile = new File(parent,fm.getInputFileName());
					if(fm.getInputFileName().equalsIgnoreCase(inputJson))
					{
						ExternalFileSchema schema = ExternalFileSchema.load(fm.inputFileStream, Charset.forName("UTF-8"), System.out);
						fm.setInputFileName(ExternalFileSchema.getSchemaFile(new File(inputCsv), System.out).getName());
						outFile = new File(parent,fm.getInputFileName());
						ExternalFileSchema.save(outFile, schema, System.out);
					}else
					{
						FileOutputStream out = new FileOutputStream(outFile);
						IOUtils.copy(fm.inputFileStream, out);
						out.close();
					}
					fm.savedFile = outFile;
					fm.setInputFileSize(outFile.length()+"");
					if(outFile.length()==0)
					{
						throw new IllegalArgumentException("Input File {"+outFile.getName()+"} is of zero length");
					}
				}
		}
		return files;
	}
}
