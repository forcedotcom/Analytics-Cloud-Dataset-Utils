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
package com.sforce.dataset;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;


public class DatasetUtilConstants {
	
	public static final int DEFAULT_BUFFER_SIZE = 8*1024*1024;
	
	public static boolean createNewDateParts = false;

	public static final String defaultEndpoint = "https://login.salesforce.com/services/Soap/u/32.0";
	public static final String defaultTestEndpoint = "https://test.salesforce.com/services/Soap/u/32.0";
	public static final String defaultSoapEndPointPath = "/services/Soap/u/32.0";
	
	public static boolean debug = false;
	public static boolean ext = false;
	
	public static final String defaultAppName = "My Private App";

	public static File currentDir =  new File("").getAbsoluteFile();
	public static final String logsDirName = "logs";
	public static final String successDirName = "sucess";
	public static final String errorDirName = "error";
	public static final String dataDirName = "data";
	public static final String configDirName = "config";
	
	public static final String errorCsvParam = "ERROR_CSV";
	public static final String metadataJsonParam = "METADATA_JSON";
	public static final String hdrIdParam = "HEADER_ID";
	public static final String serverStatusParam = "SERVER_STATUS";
	
	
	
	public static final File getOrgDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File orgDir = new File(DatasetUtilConstants.currentDir,"Org-"+orgId);
		try {
			FileUtils.forceMkdir(orgDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return orgDir;
	}
	
	public static final File getLogsDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File logsDir = new File(getOrgDir(orgId),logsDirName);
		try {
			FileUtils.forceMkdir(logsDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logsDir;
	}
	

	public static final File getSuccessDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File logsDir = new File(getOrgDir(orgId),successDirName);
		try {
			FileUtils.forceMkdir(logsDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logsDir;
	}

	public static final File getErrorDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File logsDir = new File(getOrgDir(orgId),errorDirName);
		try {
			FileUtils.forceMkdir(logsDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logsDir;
	}

	public static final File getDataDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File logsDir = new File(getOrgDir(orgId),dataDirName);
		try {
			FileUtils.forceMkdir(logsDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logsDir;
	}

	public static final File getConfigDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File logsDir = new File(getOrgDir(orgId),configDirName);
		try {
			FileUtils.forceMkdir(logsDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logsDir;
	}

}