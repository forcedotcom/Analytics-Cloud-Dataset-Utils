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
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.flow.monitor.ThreadContext;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.dataset.loader.DatasetLoaderException;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;



public class CsvUploadWorker implements Runnable {

//	private static final String success = "Success";
//	private static final String error = "Error";

//	private final File fileDir;
	private final PartnerConnection partnerConnection;
//	private final File errorDir;
//	private final File successDir;
//	private final File logsDir;
	private final File logFile;
	private final Session session;

	private Charset inputFileCharset = null;
	private final PrintStream logger;
	private File csvFile = null;
	private String datasetName = null;
	private String datasetLabel = null;
	private String datasetApp = null;
	private String operation = "Overwrite"; 
	private String uploadFormat = "binary"; 
	private boolean useBulkAPI = false;
	private String notificationLevel = null; 
	private String notificationEmail = null;
	private int chunkSizeMulti =1;
	
	private volatile AtomicBoolean uploadStatus = new AtomicBoolean(false);
	private volatile AtomicBoolean isDone = new AtomicBoolean(false);
	private String mode = null;
	
	public CsvUploadWorker(List<FileUploadRequest> requestFiles, PartnerConnection partnerConnection,Session session) throws IOException, ConnectionException
	{
		this.partnerConnection = partnerConnection;
		this.session = session;
		@SuppressWarnings("unused")
		File jsonFile = null;
//		FileUploadRequest jsonFileParam = null;
//		FileUploadRequest logFileParam = null;
		for(FileUploadRequest inputFile:requestFiles)
		{
			if(inputFile.getInputCsv() != null)
			{
				if(inputFile.getInputCsv().equalsIgnoreCase(inputFile.getInputFileName()))
				{
					this.csvFile = inputFile.savedFile;
//					this.inputFileCharset = Charset.forName(inputFile.getInputFileCharset());
					this.datasetName = inputFile.getDatasetName();
					this.datasetLabel = inputFile.getDatasetLabel();
					this.datasetApp = inputFile.getDatasetApp();
					this.operation = inputFile.getOperation();
					this.mode = inputFile.getMode();
					inputFile.setInputFileType("Csv");
					
					if(inputFile.getInputFileCharset()!=null && !inputFile.getInputFileCharset().trim().isEmpty() && !inputFile.getInputFileCharset().equalsIgnoreCase("auto"))
					{
						try
						{
							this.inputFileCharset = Charset.forName(inputFile.getInputFileCharset());
						} catch (Throwable  e) {
						}
					}
				}
			}

			if(inputFile.getInputJson()!= null)
			{
				if(inputFile.getInputJson().equalsIgnoreCase(inputFile.getInputFileName()))
				{
					jsonFile = inputFile.savedFile;
//					jsonFileParam = inputFile;
//					jsonFileParam.setInputFileType("Json");
				}
			}
		}
		
		if(this.csvFile==null)
		{
			throw new IOException("CSV file not found in Upload Request");
		}
		


		
//		String orgId = null;
//		orgId = partnerConnection.getUserInfo().getOrganizationId();

//		this.fileDir = csvFile.getAbsoluteFile().getParentFile();
//		this.errorDir = new File(fileDir,success); 
//		this.successDir = new File(fileDir,error); 
//		this.logsDir = new File(fileDir, sessionLog);
//		FileUtils.forceMkdir(errorDir);
//		FileUtils.forceMkdir(successDir);
//		FileUtils.forceMkdir(logsDir);

//		this.session = new Session(orgId,datasetName);
//		this.session.setName(datasetName);
		this.logFile = session.getSessionLog();
//		this.logFile = new File(logsDir,FilenameUtils.getBaseName(csvFile.getName())+timeStamp+".log");
		this.logger = new PrintStream(new FileOutputStream(logFile), true, "UTF-8");
//		ExternalFileSchema schema = ExternalFileSchema.init(csvFile, inputFileCharset, logger);
//		if(schema == null)
//		{
//			throw new IllegalArgumentException("Failed to generate  schema for File {"+csvFile+"}");
//		}
//		File schemaFile = ExternalFileSchema.getSchemaFile(csvFile, logger);
//		if(schemaFile == null || !schemaFile.exists() || schemaFile.length() == 0)
//		{
//			throw new IllegalArgumentException("Failed to generate  schema for File {"+csvFile+"}");
//		}
//		
//		if((this.operation.equalsIgnoreCase("Upsert") || this.operation.equalsIgnoreCase("Delete")) && !ExternalFileSchema.hasUniqueID(schema))
//		{
//			throw new IllegalArgumentException("Schema File {"+schemaFile+"} must have uniqueId set for atleast one field");
//		}
		
//		if(jsonFileParam==null)
//		{
//			jsonFileParam = new FileUploadRequest();
//			jsonFileParam.setDatasetName(datasetName);
//			jsonFileParam.setDatasetLabel(datasetLabel);
//			jsonFileParam.setDatasetApp(datasetApp);
//			jsonFileParam.setInputFileCharset(inputFileCharset.name());
//			jsonFileParam.setInputFileName(schemaFile.getName());
//			jsonFileParam.savedFile = schemaFile;
//			jsonFileParam.setInputFileSize(schemaFile.length()+"");
//			jsonFileParam.setInputJson(schemaFile.getName());
//			jsonFileParam.setOperation(operation);
//			jsonFileParam.setInputFileType("Json");
//			requestFiles.add(jsonFileParam);
//		}
//
//		logFileParam = new FileUploadRequest();
//		logFileParam.setDatasetName(datasetName);
//		logFileParam.setDatasetLabel(datasetLabel);
//		logFileParam.setDatasetApp(datasetApp);
//		logFileParam.setInputFileCharset(inputFileCharset.name());
//		logFileParam.setInputFileName(logFile.getName());
//		logFileParam.savedFile = logFile;
//		logFileParam.setInputFileSize(logFile.length()+"");
//		logFileParam.setOperation(operation);
//		logFileParam.setInputFileType("Log");
//		requestFiles.add(logFileParam);		
	}
	
	@Override
	public void run() {
        ThreadContext threadContext = ThreadContext.get();
        threadContext.setSession(session);
        session.start();
		boolean status = false;
		try {
			status = DatasetLoader.uploadDataset(csvFile.toString(), null ,uploadFormat, CodingErrorAction.REPORT, inputFileCharset, datasetName, datasetApp, datasetLabel, operation, useBulkAPI,
					chunkSizeMulti,partnerConnection, notificationLevel, notificationEmail, mode, logger);
			if(status)
				session.end();
			else
				session.fail("Check sessionLog for details");
		} catch (DatasetLoaderException e) {
			e.printStackTrace(logger);
			session.fail(e.getMessage());
		}
		moveInputFile(csvFile, status, session);
		uploadStatus.set(status);
		isDone.set(true);
	}
	
	public boolean isDone() {
		return isDone.get();
	}

	public boolean isUploadStatus() {
		return uploadStatus.get();
	}

	public static void moveInputFile(File inputFile, boolean isSuccess, Session session) 
	{
		if(inputFile == null)
			return;

		if(!inputFile.exists())
			return;

		if(inputFile.isDirectory())
			return;

//		File parent = inputFile.getAbsoluteFile().getParentFile();

		File directory = DatasetUtilConstants.getSuccessDir(session.getOrgId());
		if(!isSuccess)
			directory = DatasetUtilConstants.getErrorDir(session.getOrgId());
			
//		File doneFile = new File(directory, FilenameUtils.getBaseName(inputFile.getName())+"_"+session.getId()+"."+FilenameUtils.getExtension(inputFile.getName()));
		File doneFile = new File(directory, inputFile.getName());
		try {
			FileUtils.moveFile(inputFile, doneFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File sortedtFile = new File(inputFile.getParent(), FilenameUtils.getBaseName(inputFile.getName())+ "_sorted." + FilenameUtils.getExtension(inputFile.getName()));
		if(sortedtFile.exists())
		{
//			File sortedDoneFile = new File(directory,FilenameUtils.getBaseName(sortedtFile.getName())+"_"+session.getId()+"."+FilenameUtils.getExtension(sortedtFile.getName()));
			File sortedDoneFile = new File(directory,sortedtFile.getName());
			try {
				FileUtils.moveFile(sortedtFile, sortedDoneFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	public File getLogFile() {
		return logFile;
	}
	

}
