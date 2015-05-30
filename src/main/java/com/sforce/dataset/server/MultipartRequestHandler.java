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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;

public class MultipartRequestHandler {
	

	public static List<FileItem> getUploadRequestFileItems(HttpServletRequest request) throws FileUploadException {
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);
		if (isMultipart) {
			DiskFileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload upload = new ServletFileUpload(factory);
			List<FileItem> items = upload.parseRequest(request);
			return items;
		}
		throw new IllegalArgumentException("Request is not multipart");
	}
	
	public static String getDatasetName(List<FileItem> items) 
	{	
		for (FileItem item : items) {
			if (item.isFormField()) {
				if (item.getFieldName().equalsIgnoreCase("DatasetName")) {
					String datasetName = item.getString();
					String santizedDatasetName = ExternalFileSchema.createDevName(item.getString(), "Dataset", 1, false);
					if(!datasetName.equals(santizedDatasetName))
					{
						throw new IllegalArgumentException("dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
					}
					return datasetName;
				}
			}
		}
		throw new IllegalArgumentException("Parameter 'DatasetName' not found in FileUpload Request");
	}
	
	public static boolean isPreview(List<FileItem> items) 
	{	
		for (FileItem item : items) 
		{
			if (item.isFormField()) 
			{
				if (item.getFieldName().equalsIgnoreCase("preview")) 
				{
					String preview = item.getString();
					if(preview.equalsIgnoreCase("true"))
					{
						return true;
					}
				}
			}
		}
		return false;
	}


	
	public static List<FileUploadRequest> uploadByApacheFileUpload(List<FileItem> items,File datadir, Session session) throws IOException
	{
		List<FileUploadRequest> files = new LinkedList<FileUploadRequest>();
		FileUploadRequest temp = null;
		String datasetName = null;
		String datasetLabel = null;
		String datasetApp = null;
		String operation = null;
		String inputCsv = null;
		String inputJson = null;
		String inputFileCharset = null;
				
		for (FileItem item : items) {
				    if (item.isFormField()) {
				        if(item.getFieldName().equals("DatasetName"))
				        {
				        	datasetName = item.getString();
				    		if(datasetName!=null && !datasetName.isEmpty())
				    		{
				    			datasetLabel = datasetName;
				    			datasetName = ExternalFileSchema.createDevName(datasetName, "Dataset", 1, false);
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
				        if(item.getFieldName().equals("inputFileCharset"))
				        	inputFileCharset = item.getString();
				        
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
//					File outFile = new File(parent,fm.getInputFileName());
					String fileExt = "csv";
					if(fm.getInputFileName()!=null && !fm.getInputFileName().isEmpty())
						fileExt = FilenameUtils.getExtension(fm.getInputFileName());
					if(fileExt == null || fileExt.isEmpty())
						fileExt = "csv";
					File outFile = new File(parent,datasetName+"_"+session.getId()+"."+fileExt);
					if(fm.getInputFileName().equalsIgnoreCase(inputJson))
					{
						ExternalFileSchema schema = ExternalFileSchema.load(fm.inputFileStream, Charset.forName("UTF-8"), System.out);
//						fm.setInputFileName(ExternalFileSchema.getSchemaFile(new File(inputCsv), System.out).getName());
						fm.setInputFileName(ExternalFileSchema.getSchemaFile(outFile, System.out).getName());
						fm.setInputJson(fm.getInputFileName());
						outFile = new File(parent,fm.getInputFileName());
						try
						{
							DatasetUtilConstants.ext = true;
							ExternalFileSchema.save(outFile, schema, System.out);
						}finally
						{
							DatasetUtilConstants.ext = false;					
						}
					}else
					{
						FileOutputStream fos = null;
						BufferedOutputStream bos = null;
						try
						{
							fos = new FileOutputStream(outFile);
							bos = new BufferedOutputStream(fos,DatasetUtilConstants.DEFAULT_BUFFER_SIZE);
							IOUtils.copy(fm.inputFileStream, bos);
						}finally
						{
							if(bos!=null)
								IOUtils.closeQuietly(bos);
							if(fos!=null)
								IOUtils.closeQuietly(fos);
						}
						fm.setInputFileName(outFile.getName());
						fm.setInputCsv(outFile.getName());
						fm.setInputFileCharset(inputFileCharset);
					}
					fm.savedFile = outFile;
					fm.setInputFileSize(outFile.length()+"");
					if(outFile.length()==0)
					{
						throw new IllegalArgumentException("Input File {"+outFile.getName()+"} is of zero length");
					}
				}
		return files;
	}
	
	public static File saveFile(List<FileItem> items,File datadir) throws IOException
	{
		File inputFile = null;
		for (FileItem item : items) 
		{				    
		    if (!item.isFormField()) 
		    {
		    	if(item.getFieldName().equals("inputCsv") && item.getInputStream() != null)
		    	{
						inputFile = new File(datadir,item.getName());
						FileOutputStream fos = null;
						BufferedOutputStream bos = null;
						try
						{
							fos = new FileOutputStream(inputFile);
							bos = new BufferedOutputStream(fos,DatasetUtilConstants.DEFAULT_BUFFER_SIZE);
							IOUtils.copy(item.getInputStream(),bos);
						}finally
						{
							if(bos!=null)
								IOUtils.closeQuietly(bos);
							if(fos!=null)
								IOUtils.closeQuietly(fos);
						}
		    	}
		    }
		}
		if(inputFile != null && inputFile.length()==0)
		{
			throw new IllegalArgumentException("Input File {"+inputFile.getName()+"} is of zero length");
		}				
		return inputFile;
	}
	
}
