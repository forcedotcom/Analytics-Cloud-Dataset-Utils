package com.sforce.dataset.loader.file.listener;

import java.io.File;
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
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class FileListener {
	private String datasetAlias = null;
	private String datasetLabel = null;
	private String datasetApp = null;
	private String inputFileDirectory = null;
	private String inputFilePattern = null;
	private String operation = "Overwrite"; 
	private String filecharset = null;
	private String uploadFormat = "binary"; 
	private String codingErrorAction = "REPORT";
	private boolean useBulkAPI = false;
	private int pollingInterval = 10000;
	private int fileAge = 10000;
	private int chunkSizeMulti =5;
	private String notificationLevel = null; 
	private String notificationEmail = null;

	
	@JsonIgnore
	Charset charset = null;
	
	@JsonIgnore
	File fileDir = null;
	
	@JsonIgnore
	CodingErrorAction cea = CodingErrorAction.REPORT;
	
	
	@JsonIgnore
	public CodingErrorAction getCea() {
		return cea;
	}
	
	@JsonIgnore
	public void setCea(CodingErrorAction cea) {
		this.cea = cea;
	}
	
	public String getDatasetLabel() {
		return datasetLabel;
	}
	public void setDatasetLabel(String datasetLabel) {
		this.datasetLabel = datasetLabel;
	}
	public String getFilecharset() {
		return filecharset;
	}
	public void setFilecharset(String filecharset) {
		if(filecharset!=null && !filecharset.trim().isEmpty())
		{
			charset = Charset.forName(filecharset);
			this.filecharset = filecharset;
		}
	}
	public String getUploadFormat() {
		return uploadFormat;
	}
	public void setUploadFormat(String uploadFormat) {
		if(uploadFormat !=null)
		{
			if(uploadFormat.equalsIgnoreCase("CSV") || uploadFormat.equalsIgnoreCase("BINARY"))
				this.uploadFormat = uploadFormat;
		}
	}
	public String getCodingErrorAction() {
		return codingErrorAction;
	}
	public void setCodingErrorAction(String codingErrorAction) {
		if(codingErrorAction != null)
		{
			if(codingErrorAction.equalsIgnoreCase("REPLACE"))
			{
				cea = CodingErrorAction.REPLACE;
				this.codingErrorAction = codingErrorAction;
			}
			else if(codingErrorAction.equalsIgnoreCase("IGNORE"))
			{
				cea = CodingErrorAction.IGNORE;
				this.codingErrorAction = codingErrorAction;
			}
		}
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		if(operation != null)
		{
			if(operation.equalsIgnoreCase("OVERWRITE"))
			{
				this.operation = "OVERWRITE";
			}
			else if(operation.equalsIgnoreCase("APPEND"))
			{
				this.operation = "APPEND";
			}
			else if(operation.equalsIgnoreCase("UPSERT"))
			{
				this.operation = "UPSERT";
			}
			else if(operation.equalsIgnoreCase("DELETE"))
			{
				this.operation = "DELETE";
			}
		}
	}
	public boolean isUseBulkAPI() {
		return useBulkAPI;
	}
	public void setUseBulkAPI(boolean useBulkAPI) {
		this.useBulkAPI = useBulkAPI;
	}
	public int getChunkSizeMulti() {
		return chunkSizeMulti;
	}
	public String getInputFileDirectory() {
		return inputFileDirectory;
	}
	
	public void setInputFileDirectory(String inputFileDirectory) {
		if(inputFileDirectory!=null)
		{
			File temp = new File(inputFileDirectory);
			String pattern = null;
			if(temp.exists())
			{
				if(!temp.isDirectory())
				{
					temp = temp.getAbsoluteFile().getParentFile();
					if(temp==null)
					{
						throw new IllegalArgumentException("Invalid inputFileDirectory {"+inputFileDirectory+"}");
					}
					pattern = temp.getName();
				}
				
				if(temp.getAbsoluteFile().getParentFile()==null)
				{
					throw new IllegalArgumentException("Invalid inputFileDirectory {"+inputFileDirectory+"}, directory cannot be top level directory");
				}
				
				this.fileDir = temp;
				this.inputFileDirectory = temp.getAbsolutePath();
				if(pattern!=null)
				{
					setInputFilePattern(pattern);
				}
			}
		}else
		{
			throw new IllegalArgumentException("inputFileDirectory cannot be null");
		}
	}
	
	public String getInputFilePattern() {
		if(inputFilePattern!=null && !inputFilePattern.isEmpty())
		{
			inputFilePattern = "*.csv";
		}
		return inputFilePattern;
	}

	public void setInputFilePattern(String inputFilePattern) {
		if(inputFilePattern!=null && !inputFilePattern.isEmpty())
		{
			this.inputFilePattern = inputFilePattern;
		}else
		{
			throw new IllegalArgumentException("inputFilePattern cannot be null");
		}
	}

	public int getPollingInterval() {
		return pollingInterval;
	}

	public void setPollingInterval(int pollingInterval) {
		this.pollingInterval = pollingInterval;
	}

	public int getFileAge() {
		return fileAge;
	}

	public void setFileAge(int fileAge) {
		this.fileAge = fileAge;
	}

	public String getUri() {
		if(this.fileDir!=null)
		{
			return this.fileDir.toURI().toASCIIString() + "("+inputFilePattern!=null?inputFilePattern:"*"+")";
		}
		return null;
	}

	public String getDatasetAlias() {
		return datasetAlias;
	}

	public void setDatasetAlias(String datasetAlias) {
		this.datasetAlias = datasetAlias;
	}

	public String getDatasetApp() {
		return datasetApp;
	}

	public void setDatasetApp(String datasetApp) {
		this.datasetApp = datasetApp;
	}

	public String getNotificationLevel() {
		return notificationLevel;
	}

	public void setNotificationLevel(String notificationLevel) {
		this.notificationLevel = notificationLevel;
	}

	public String getNotificationEmail() {
		return notificationEmail;
	}

	public void setNotificationEmail(String notificationEmail) {
		this.notificationEmail = notificationEmail;
	}

}
