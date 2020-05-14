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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BOMInputStream;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.loader.file.schema.ext.DetectFieldTypes;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.HttpUtils;
import com.sforce.dataset.util.SortSimpleDateFormat;


public class DatasetUtilConstants {
	
	@SuppressWarnings("unused")
	private static final boolean isJdk14LoggerConfigured = DatasetUtils.configureLog4j();	
	public static final int DEFAULT_BUFFER_SIZE = 8*1024*1024;
	
//	public static boolean createNewDateParts = false;
	public static CodingErrorAction codingErrorAction = CodingErrorAction.REPORT;

	public static final String defaultEndpoint = "https://login.salesforce.com/services/Soap/u/48.0";
	public static final String defaultTestEndpoint = "https://test.salesforce.com/services/Soap/u/48.0";
	public static final String defaultSoapEndPointPath = "/services/Soap/u/48.0";
	
	public static boolean debug = false;
	public static boolean ext = false;
	
	public static final String defaultAppName = "My Private App";

	public static final String logsDirName = "logs";
	public static final String successDirName = "success";
	public static final String errorDirName = "error";
	public static final String dataDirName = "data";
	public static final String configDirName = "config";
	public static final String configFileName = "settings.json";
	public static final String preferenceFileName = "preferences.json";
	public static final String debugFileName = "debug.log";
	public static final String dataflowDirName = "dataflow";
	public static final String dataflowGroupDirName = "dataflowgroup";
	public static final String scheduleDirName = "schedule";
	public static final String listenerDirName = "listener";
	public static final String dateFormatsFileName = "dateFormats.json";
	

	

	public static final String errorCsvParam = "ERROR_CSV";
	public static final String metadataJsonParam = "METADATA_JSON";
	public static final String hdrIdParam = "HEADER_ID";
	public static final String serverStatusParam = "SERVER_STATUS";
	public static final String clientId = "com.sforce.dataset.utils";
	
	public static final int max_error_threshhold = 10000;

	public static boolean server = true;
	
	static com.sforce.dataset.Config systemConfig = null;
	
	private static File currentDir =  new File("").getAbsoluteFile();
	private static File userDir =  new File(System.getProperty("user.home"), "DatasetUtils").getAbsoluteFile();
	static
	{
		if(!userDir.exists())
		{
			try {
				FileUtils.forceMkdir(userDir);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}
	
	public static final File getAppDir()
	{
		if(server && userDir.isDirectory())
		{	
			return userDir;
		}
		return currentDir;
	}
	
	public static final File getOrgDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File orgDir = new File(DatasetUtilConstants.getAppDir(),"Org-"+orgId);
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

	public static final File getDataflowDir(String orgId)
	{
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File dataflowDir = new File(getOrgDir(orgId),dataflowDirName);
		try {
			FileUtils.forceMkdir(dataflowDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataflowDir;
	}
	
	public static File getDataflowGroupDir(String orgId) {
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File dataflowDir = new File(getOrgDir(orgId),dataflowGroupDirName);
		try {
			FileUtils.forceMkdir(dataflowDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataflowDir;
	}

	public static File getScheduleDir(String orgId) {
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File dataflowDir = new File(getOrgDir(orgId),scheduleDirName);
		try {
			FileUtils.forceMkdir(dataflowDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataflowDir;
	}

	public static File getListenerDir(String orgId) {
		if(orgId == null || orgId.isEmpty())
			throw new IllegalArgumentException("orgId is null");
		File dataflowDir = new File(getOrgDir(orgId),listenerDirName);
		try {
			FileUtils.forceMkdir(dataflowDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataflowDir;
	}

	
	public static final File getDebugFile()
	{
		File logsDir = new File(DatasetUtilConstants.getAppDir(),logsDirName);
		try {
			FileUtils.forceMkdir(logsDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File debugFile = new File(logsDir,debugFileName);
		return debugFile;
	}
	
	
	public static String getOauthClientId()
	{
        try {
            Properties versionProps = new Properties();
            versionProps.load(DatasetUtilMain.class.getClassLoader().getResourceAsStream("config.properties"));
            if(versionProps.containsKey("oauth.client_id"))
            	return versionProps.getProperty("oauth.client_id");
        } catch (Throwable t) {
        	t.printStackTrace();
        }
        throw new IllegalStateException("OAuth is not supported in this build");
     }
	
	public static String getOauthRedirectURI()
	{
        try {
            Properties versionProps = new Properties();
            versionProps.load(DatasetUtilMain.class.getClassLoader().getResourceAsStream("config.properties"));
            if(versionProps.containsKey("oauth.redirect_uri"))
            	return versionProps.getProperty("oauth.redirect_uri");
        } catch (Throwable t) {
        	t.printStackTrace();
        }
        throw new IllegalStateException("OAuth is not supported in this build");
     }

	public static final Config getSystemConfig()
	{
		if(systemConfig==null)
		{
			Config conf = new Config();
			File configDir = new File(DatasetUtilConstants.getAppDir(),configDirName);
			try {
				FileUtils.forceMkdir(configDir);
			} catch (IOException e) {
				e.printStackTrace();
			}
			File configFile = new File(configDir,configFileName);
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			if(configFile != null && configFile.exists())
			{
				InputStreamReader reader = null;
				try {
					reader = new InputStreamReader(new BOMInputStream(new FileInputStream(configFile), false), DatasetUtils.utf8Decoder(null , Charset.forName("UTF-8")));
					conf  =  mapper.readValue(reader, Config.class);						
				} catch (Throwable e) {
					e.printStackTrace();
				}finally
				{
					IOUtils.closeQuietly(reader);
				}
			}else
			{
				try
				{
					mapper.writerWithDefaultPrettyPrinter().writeValue(configFile, conf);
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
			systemConfig = conf;
		}
		return systemConfig;
	}
	
	public static final void setSystemConfig(String proxyUsername,
	String proxyPassword,
	String proxyNtlmDomain,
	String proxyHost,
	int proxyPort,
	int timeoutSecs,
	int connectionTimeoutSecs,
	boolean noCompression,
	boolean debugMessages) throws JsonGenerationException, JsonMappingException, IOException, URISyntaxException
	{
		Config conf = new Config();
		conf.proxyUsername = proxyUsername;
		conf.proxyPassword = proxyPassword;
		conf.proxyNtlmDomain = proxyNtlmDomain;
		conf.proxyHost = proxyHost;
		conf.proxyPort = proxyPort;
		conf.timeoutSecs = timeoutSecs;
		conf.noCompression = noCompression;
		conf.debugMessages = debugMessages;
		
		HttpUtils.testProxyConfig(conf);
		
		File configDir = new File(DatasetUtilConstants.getAppDir(),configDirName);
		try {
			FileUtils.forceMkdir(configDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File configFile = new File(configDir,configFileName);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.writerWithDefaultPrettyPrinter().writeValue(configFile, conf);
		
		systemConfig = conf;
	}
	
	public static final void setPreferences(String orgId,
	String notificationLevel,
	String notificationEmail,
	int fiscalMonthOffset,
	int firstDayOfWeek,
	boolean isYearEndFiscalYear) throws JsonGenerationException, JsonMappingException, IOException
	{
		
		if(notificationLevel!=null)
		{
			if(!notificationLevel.equalsIgnoreCase("always") && 
					!notificationLevel.equalsIgnoreCase("failures") &&
					!notificationLevel.equalsIgnoreCase("warnings") &&
					!notificationLevel.equalsIgnoreCase("never"))
			{
				throw new IllegalArgumentException("notificationLevel '"+notificationLevel+"' is not a valid value");
			}
		}else
		{
			throw new IllegalArgumentException("notificationLevel '"+notificationLevel+"' is not a valid value");
		}

		if(notificationEmail!=null)
		{
			if(notificationEmail.equals("current.user@yourdomain.com"))
				notificationEmail = null;
		}
		
		if(fiscalMonthOffset<0 || fiscalMonthOffset>11)
		{
			throw new IllegalArgumentException("fiscalMonthOffset '"+fiscalMonthOffset+"' is not a valid value");						
		}
			
		if(firstDayOfWeek<-1 && firstDayOfWeek>6)
		{
			throw new IllegalArgumentException("firstDayOfWeek '"+firstDayOfWeek+"' is not a valid value");						
		}

		Preferences pref = new Preferences();
		pref.firstDayOfWeek = firstDayOfWeek;
		pref.fiscalMonthOffset = fiscalMonthOffset;
		pref.isYearEndFiscalYear = isYearEndFiscalYear;
		pref.notificationEmail = notificationEmail;
		pref.notificationLevel = notificationLevel;

		File configDir = DatasetUtilConstants.getConfigDir(orgId);
		if(!configDir.isDirectory())
		{
			FileUtils.forceMkdir(configDir);
		}
		File configFile = new File(configDir,preferenceFileName);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.writerWithDefaultPrettyPrinter().writeValue(configFile, pref);
	}
	
	public static final Preferences getPreferences(String orgId)
	{
		Preferences pref = new Preferences();
		File configDir = DatasetUtilConstants.getConfigDir(orgId);
		try {
			FileUtils.forceMkdir(configDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File configFile = new File(configDir,preferenceFileName);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		if(configFile != null && configFile.exists())
		{
			InputStreamReader reader = null;
			try {
				reader = new InputStreamReader(new BOMInputStream(new FileInputStream(configFile), false), DatasetUtils.utf8Decoder(null , Charset.forName("UTF-8")));
				pref  =  mapper.readValue(reader, Preferences.class);						
			} catch (Throwable e) {
				e.printStackTrace();
			}finally
			{
				IOUtils.closeQuietly(reader);
			}
		}else
		{
			try
			{
				mapper.writerWithDefaultPrettyPrinter().writeValue(configFile, pref);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		return pref;
	}

	public static LinkedHashSet<SimpleDateFormat> getSuportedDateFormats() 
	{
		LinkedHashSet<SimpleDateFormat> dateFormats = new LinkedHashSet<SimpleDateFormat>();
		File configDir = new File(DatasetUtilConstants.getAppDir(),configDirName);
		try {
			FileUtils.forceMkdir(configDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File configFile = new File(configDir,dateFormatsFileName);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		if(configFile != null && configFile.exists())
		{
			InputStreamReader reader = null;
			try {
				reader = new InputStreamReader(new BOMInputStream(new FileInputStream(configFile), false), DatasetUtils.utf8Decoder(null , Charset.forName("UTF-8")));
				SupportedDateFormatType dateFormatsList = mapper.readValue(reader, SupportedDateFormatType.class);
				if(dateFormatsList.supportedDateFormatList != null && !dateFormatsList.supportedDateFormatList.isEmpty())
				{
				    Collections.sort(dateFormatsList.supportedDateFormatList, new SortSimpleDateFormat());
					for(String dateFormat:dateFormatsList.supportedDateFormatList)
					{
				         try
				         {
				        	 SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
					   	     SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
						     tempSdf.setLenient(false);
						     dateFormats.add(tempSdf);
				         }catch(Throwable t1)
				         {
				        	 t1.printStackTrace();
				         }					
				     }
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}finally
			{
				IOUtils.closeQuietly(reader);
			}
		}
		
		if(dateFormats == null || dateFormats.isEmpty())
		{
			LinkedHashSet<SimpleDateFormat> temp = DetectFieldTypes.getSuportedDateFormats();
			List<String> dateFormatsList= new ArrayList<String>();
			for(SimpleDateFormat sdf:temp)
			{
				dateFormatsList.add(sdf.toPattern());
			}

			try
			{
			    Collections.sort(dateFormatsList, new SortSimpleDateFormat());
			} catch (Throwable e) {
				e.printStackTrace();
			}
			
			for(String dateformat:dateFormatsList)
			{
				SimpleDateFormat tmp = new SimpleDateFormat(dateformat);
				tmp.setLenient(false);
				dateFormats.add(tmp);
			}
		}
		return dateFormats;		
	}
	
}
