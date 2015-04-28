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
	private String dataset = null;
	private String datasetLabel = null;
	private String app = null;
	private String filecharset = "UTF-8";
	private String uploadFormat = "binary"; 
	private String codingErrorAction = "REPORT";
	private String operation = "Overwrite"; 
	private boolean useBulkAPI = false;
	private String inputFileDirectory = null;
	private String inputFilePattern = null;
	private int pollingInterval = 10000;
	private int fileAge = 10000;
	
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
	
	public String getDataset() {
		return dataset;
	}
	public void setDataset(String dataset) {
		this.dataset = dataset;
	}
	public String getDatasetLabel() {
		return datasetLabel;
	}
	public void setDatasetLabel(String datasetLabel) {
		this.datasetLabel = datasetLabel;
	}
	public String getApp() {
		return app;
	}
	public void setApp(String app) {
		this.app = app;
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
	public String getInputFileDirectory() {
		return inputFileDirectory;
	}
	
	public void setInputFileDirectory(String inputFileDirectory) {
		if(inputFileDirectory!=null)
		{
			File temp = new File(inputFileDirectory);
			if(temp.exists())
			{
				if(temp.isDirectory())
				{
					this.inputFileDirectory = inputFileDirectory;
					this.fileDir = temp;
				}else
				{
					this.fileDir = temp.getAbsoluteFile().getParentFile();
					if(this.fileDir != null)
						this.inputFileDirectory = this.fileDir.toString();
					setInputFilePattern(temp.getName());
				}
			}
		}else
		{
			throw new IllegalArgumentException("inputFileDirectory cannot be null");
		}
	}
	
	public String getInputFilePattern() {
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
}
