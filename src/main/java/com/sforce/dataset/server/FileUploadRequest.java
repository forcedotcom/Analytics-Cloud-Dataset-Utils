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
import java.io.InputStream;



public class FileUploadRequest {
	
	
	private String datasetName = null;
	private String datasetLabel = null;
	private  String datasetApp = null;
	private  String operation = null;
	private  String inputCsv = null;
	private  String inputJson = null;

	private  String inputFileName = null;
	private  String inputFileSize = null;
	private  String inputFileType = null;
	private  String inputFileCharset = "UTF-8";
	private String mode = null;
	public   File savedFile = null;
	
	public transient InputStream inputFileStream;
	
	public String getDatasetName() {
		return datasetName;
	}
	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}
	public String getDatasetApp() {
		return datasetApp;
	}
	public void setDatasetApp(String datasetApp) {
		this.datasetApp = datasetApp;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getInputCsv() {
		return inputCsv;
	}
	public void setInputCsv(String inputCsv) {
		this.inputCsv = inputCsv;
	}
	public String getInputJson() {
		return inputJson;
	}
	public void setInputJson(String inputJson) {
		this.inputJson = inputJson;
	}
	public String getInputFileSize() {
		return inputFileSize;
	}
	public void setInputFileSize(String inputFileSize) {
		this.inputFileSize = inputFileSize;
	}
	public String getInputFileType() {
		return inputFileType;
	}
	public void setInputFileType(String inputFileType) {
		this.inputFileType = inputFileType;
	}
	public String getInputFileName() {
		return inputFileName;
	}
	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}
	public String getInputFileCharset() {
		return inputFileCharset;
	}
	public void setInputFileCharset(String inputFileCharset) {
		this.inputFileCharset = inputFileCharset;
	}
	public String getDatasetLabel() {
		return datasetLabel;
	}
	public void setDatasetLabel(String datasetLabel) {
		this.datasetLabel = datasetLabel;
	}
	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
}
