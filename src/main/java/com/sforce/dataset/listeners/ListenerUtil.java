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
package com.sforce.dataset.listeners;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.quartz.SchedulerException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.loader.file.listener.FileListener;
import com.sforce.dataset.loader.file.listener.FileListenerUtil;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class ListenerUtil {
	
	public static List<Listener> listListeners(PartnerConnection partnerConnection) throws ConnectionException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		List<Listener> list = new LinkedList<Listener>();
		File dir = DatasetUtilConstants.getListenerDir(orgId);
		IOFileFilter suffixFileFilter = FileFilterUtils.suffixFileFilter(".json", IOCase.INSENSITIVE);
		File[] fileList =	DatasetUtils.getFiles(dir, suffixFileFilter);
		if(fileList!=null)
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 for(File file:fileList)
			 {
				 try {
					 Listener sched = mapper.readValue(file, Listener.class);
					 if(sched.getDevName()!=null)
					 {
						 list.add(sched);
					 }
				} catch (Exception e) {
					e.printStackTrace();
				}
			 }
		}
		return list;
	}
	
	public static void saveListener(PartnerConnection partnerConnection, String type, String scheduleName, Map<?,?> params,  boolean isCreate) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		Listener dg = new Listener();
		if(!isCreate)
		{
			dg = readListener(partnerConnection, scheduleName);
			dg.setParams(mergeMap(params, dg.getParams()));
		}else
		{
			String masterLabel = scheduleName;
			String devName = ExternalFileSchema.createDevName(scheduleName, "Listener", 1, false);
			dg.setDevName(devName);
			dg.setMasterLabel(masterLabel);
			dg.setType(type);
			dg.setParams(params);
		}
		dg.setLastModifiedDate(System.currentTimeMillis());
		dg.set_LastModifiedBy(partnerConnection);
		writeListener(partnerConnection, dg, isCreate);
		return;
	}

	public static void deleteListener(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getListenerDir(orgId);
		String devName = ExternalFileSchema.createDevName(scheduleName, "schedule", 1, false);
		File file = new File(dir,devName+".json");
		if(file.exists())
		{
			disableListener(partnerConnection, devName);
			file.delete();
		}else
		{
			throw new IllegalArgumentException("Listener {"+scheduleName+"} does not exist in the system");
		}
	}
	
	
	public static void disableListener(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		Listener sched = readListener(partnerConnection, scheduleName);
		stopListener(partnerConnection, sched);
    	sched.setDisabled(true);
    	writeListener(partnerConnection, sched, false);
	}
	
	public static void enableListener(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		Listener sched = readListener(partnerConnection, scheduleName);  
		startListener(sched, partnerConnection);
        sched.setDisabled(false);
        writeListener(partnerConnection, sched, false);
	}
	
	public static void writeListener(PartnerConnection partnerConnection, Listener schedule, boolean isCreate) throws ConnectionException, JsonParseException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getListenerDir(orgId);
		File file = new File(dir,schedule.getDevName()+".json");
		if(isCreate && file.exists())
		{
			throw new IllegalArgumentException("Listener {"+schedule.getDevName()+"} already exists in the system");
		}
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(file, schedule);
	}

	public static Listener readListener(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonParseException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getListenerDir(orgId);
		String devName = ExternalFileSchema.createDevName(scheduleName, "schedule", 1, false);
		File file = new File(dir,devName+".json");
		if(!file.exists())
		{
			throw new IllegalArgumentException("Listener {"+scheduleName+"} does not exist in the system");
		}
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Listener sched = mapper.readValue(file, Listener.class);
		return sched;
	}
	
	public static void startAllListeners(PartnerConnection partnerConnection) throws ConnectionException, SchedulerException
	{
		List<Listener> list = listListeners(partnerConnection);
		for(Listener sched:list)
		{
			if(!sched.isDisabled())
			{
		        if(sched.getType().equalsIgnoreCase("file"))
		        {
					try
					{
			    	    if(!FileListenerUtil.isRunning(sched.getDevName()))
			    	    {
			    	    	startListener(sched, partnerConnection);
			    	    }
					}catch(Exception t)
					{
						t.printStackTrace();
					}
		        }
			}
		}
	}
	
	public static void startListener(Listener sched, PartnerConnection partnerConnection) throws SchedulerException, IOException, ConnectionException
	{
	        if(sched.getType().equalsIgnoreCase("file"))
	        {
	    	    if(FileListenerUtil.isRunning(sched.getDevName()))
	    	    {
	    	    	throw new SchedulerException("Listener is already running");
	    	    }
	    	    
	    		ObjectMapper mapper = new ObjectMapper();	
	    		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	    		String params = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sched.getParams());

	    		FileListener listener = mapper.readValue(params, FileListener.class);

	    		FileListenerUtil.startListener(sched.getDevName(), listener, partnerConnection);
	        }
	}
	
	
	public static void stopListener(PartnerConnection partnerConnection, Listener sched) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
        if(sched.getType().equalsIgnoreCase("file"))
        {
        	FileListenerUtil.stopListener(sched.getDevName());
        }
	}
	
	public static Map<?,?> mergeMap(Map<?,?> newMap,Map<?,?> oldMap)
	{
		LinkedHashMap<String,Object> params = new LinkedHashMap<String,Object>();
		for(Object key:newMap.keySet())
		{
			params.put(key.toString(), newMap.get(key));
		}
		for(Object key:oldMap.keySet())
		{
			if(!params.containsKey(key))
			{
				params.put(key.toString(), oldMap.get(key));
			}
		}
		return params;
	}



}
