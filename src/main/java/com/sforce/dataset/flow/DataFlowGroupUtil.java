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
package com.sforce.dataset.flow;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class DataFlowGroupUtil {
	
	public static List<DataFlowGroup> listDataFlowGroups(PartnerConnection partnerConnection) throws ConnectionException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		List<DataFlowGroup> list = new LinkedList<DataFlowGroup>();
		File dir = DatasetUtilConstants.getDataflowGroupDir(orgId);
		IOFileFilter suffixFileFilter = FileFilterUtils.suffixFileFilter(".json", IOCase.INSENSITIVE);
		File[] fileList =	DatasetUtils.getFiles(dir, suffixFileFilter);
		if(fileList!=null)
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 for(File file:fileList)
			 {
				 try {
					 DataFlowGroup dg = mapper.readValue(file, DataFlowGroup.class);
					list.add(dg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			 }
		}
		return list;
	}
	
	public static void saveDataFlowGroups(PartnerConnection partnerConnection, String dataflowGroupName, List<String> dataflowList) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getDataflowGroupDir(orgId);
		String masterLabel = dataflowGroupName;
		String devName = ExternalFileSchema.createDevName(dataflowGroupName, "dataFlowGroup", 1, false);
		File file = new File(dir,devName+".json");
		DataFlowGroup dg = new DataFlowGroup();
		dg.setDevName(devName);
		dg.setMasterLabel(masterLabel);
		dg.setDataflowList(dataflowList);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(file, dg);
		return;
	}
	

	public static void deleteDataFlowGroups(PartnerConnection partnerConnection, String dataflowGroupName, List<String> dataflowList) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getDataflowGroupDir(orgId);
		String devName = ExternalFileSchema.createDevName(dataflowGroupName, "dataFlowGroup", 1, false);
		File file = new File(dir,devName+".json");
		if(file.exists())
		{
			file.delete();
		}else
		{
			throw new IllegalArgumentException("Dataflow Group {"+dataflowGroupName+"} does not exist in the system");
		}
	}

		
	

}
