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
package com.sforce.dataset.loader.file.listener;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.soap.partner.PartnerConnection;

public class FileListenerUtil {
	
	public static final File listenerSettingsFile = new File(".sfdc_file_listeners.json");
//	static LinkedHashMap<String,FileListener> listeners = null;

	public static FileListenerSettings getFileListeners() throws JsonParseException, JsonMappingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		if(listenerSettingsFile.exists() && listenerSettingsFile.length()>0)
		{
			FileListenerSettings listenerSettings = mapper.readValue(listenerSettingsFile, FileListenerSettings.class);
			return listenerSettings;
		}
		return null;
		
	}
	
	public static void saveFileListeners(FileListenerSettings listeners) throws JsonParseException, JsonMappingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		mapper.writerWithDefaultPrettyPrinter().writeValue(listenerSettingsFile, listeners);
	}
	
	public static boolean addListener(FileListener listener) throws JsonParseException, JsonMappingException, IOException
	{
		FileListenerSettings listeners = getFileListeners();
		if(listeners==null)
		{
			listeners = new FileListenerSettings();
		}
		if(listeners.fileListeners==null )
		{
			listeners.fileListeners = new LinkedHashMap<String,FileListener>();
		}
		if(!listeners.fileListeners.containsKey(listener.getDataset()))
		{
			listeners.fileListeners.put(listener.getDataset(), listener);
			saveFileListeners(listeners);
			return true;
		}else
		{
			System.out.println("\nERROR: FileListener for dataset {"+listener.getDataset()+"} already exists");
		}
		return false;
	}
	
	public static boolean startListener(FileListener listener, PartnerConnection partnerConnection) throws IOException
	{
		FileListenerThread fileListenerThread = new FileListenerThread(listener, partnerConnection);
		Thread th = new Thread(fileListenerThread,"FileListener-"+listener.dataset);
		th.setDaemon(true);
		th.start();
		return true;
	}

	public static boolean addAndStartListener(FileListener listener, PartnerConnection partnerConnection) throws IOException
	{
		if(addListener(listener))
			return startListener(listener, partnerConnection);
		else
			return false;
	}
	
	public static void startAllListener(PartnerConnection partnerConnection)
	{
		try 
		{
			FileListenerSettings listeners = getFileListeners();
			if(listeners!=null && listeners.fileListeners != null && !listeners.fileListeners.isEmpty())
			{
				for(String dataset:listeners.fileListeners.keySet())
				{
					startListener(listeners.fileListeners.get(dataset), partnerConnection);
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}


}
