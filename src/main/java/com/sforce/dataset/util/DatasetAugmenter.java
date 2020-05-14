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
package com.sforce.dataset.util;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.flow.node.AugmentNode;
import com.sforce.dataset.flow.node.EdgemartNode;
import com.sforce.dataset.flow.node.RegisterNode;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.metadata.DatasetSchema;
import com.sforce.soap.partner.PartnerConnection;

public class DatasetAugmenter {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void augmentEM(PartnerConnection partnerConnection) throws Exception
	{
		List<DatasetType> list = DatasetUtils.listDatasets(partnerConnection, true, null);
		System.out.println("\n");
		if(list==null || list.size()==0)
		{
			System.out.println("No dataset found in org");
			return;
		}
		int cnt = 1;
		for(DatasetType ds:list)
		{
			if(cnt==1)
				System.out.println("Found {"+list.size()+"} Datasets...");
			System.out.println(cnt+". "+ds._alias);
			cnt++;
		}
		System.out.println("\n");

		String leftDataSet = null;
		String rightDataSet = null;

		String leftDataSetId = null;
		String rightDataSetId = null;

		String leftDataSetVersion = null;
		String rightDataSetVersion = null;

		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter left Dataset Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(DatasetType ds:list)
				{
					if(cnt==left)
					{
						leftDataSet = ds._alias;
						leftDataSetId = ds._uid;
						leftDataSetVersion = ds.edgemartData._uid;
						break;
					}
					cnt++;
				}
				if(leftDataSet!=null)
					break;
			}catch(Throwable me)
			{				
				leftDataSet = null;
			}
		}

		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter Right Dataset Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(DatasetType ds:list)
				{
					if(cnt==left)
					{
						rightDataSet = ds._alias;
						rightDataSetId = ds._uid;
						rightDataSetVersion = ds.edgemartData._uid;
						break;
					}
					cnt++;
				}
				if(rightDataSet!=null)
					break;
			}catch(Throwable me)
			{				
				rightDataSet = null;
			}
		}
		System.out.println("\n");
		System.out.println("Getting dimensions in dataset {"+leftDataSet+"}");
		Map<String, String> rightXmd = null;
		Map<String, String> leftXmd = DatasetDownloader.getXMD(leftDataSet, leftDataSetId, leftDataSetVersion, partnerConnection);
		if(!leftDataSet.equals(rightDataSet))
		{
			System.out.println("Getting dimensions in dataset {"+rightDataSet+"}");
			rightXmd = DatasetDownloader.getXMD(rightDataSet, rightDataSetId, rightDataSetVersion, partnerConnection);
		}else
		{
			rightXmd = leftXmd;
		}
		Map<String, String> leftDims = removeDateParts(DatasetSchema.getDimensions(leftXmd));
		System.out.println("\n");
		if(leftDims==null || leftDims.size()==0)
		{
			System.out.println("No Dimensions found in Datasets {"+leftDataSet+"}");
			return;
		}
		cnt = 1;
		for(String dim:leftDims.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+leftDims.size()+"} Dimensions in Dataset {"+leftDataSet+"}...");
			System.out.println(cnt+". "+dim);
			cnt++;
		}
		String leftKey = null;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("(Left Key) Enter Dimension Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(String dim:leftDims.keySet())
				{
					if(cnt==left)
					{
						leftKey = dim;
						break;
					}
					cnt++;
				}
				if(leftKey!=null)
					break;
			}catch(NumberFormatException me)
			{				
				leftKey = null;
			}
		}

		Map<String, String> rightDims = removeDateParts(DatasetSchema.getDimensions(rightXmd));
		System.out.println("\n");
		if(rightDims==null || rightDims.size()==0)
		{
			System.out.println("No Dimensions found in Datasets {"+rightDataSet+"}");
			return;
		}
		cnt = 1;
		for(String dim:rightDims.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+rightDims.size()+"} Dimensions in Dataset {"+rightDataSet+"}...");
			System.out.println(cnt+". "+dim);
			cnt++;
		}
		String rightKey = null;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("(Right Key) Enter Dimension Number: ");
				long left = Long.parseLong(tmp.trim());
				cnt = 1;
				for(String dim:rightDims.keySet())
				{
					if(cnt==left)
					{
						rightKey = dim;
						break;
					}
					cnt++;
				}
				if(rightKey!=null)
					break;
			}catch(NumberFormatException me)
			{				
				rightKey = null;
			}
		}

		Map<String, String> rightFields = removeDateParts(DatasetSchema.getFields(rightXmd));		
		System.out.println("\n");
		cnt = 1;
		for(String dim:rightFields.keySet())
		{
			if(cnt==1)
				System.out.println("Found {"+rightDims.size()+"} Fields in Dataset {"+rightDataSet+"}...");
			System.out.println(cnt+". "+dim);
			cnt++;
		}

		LinkedList<String> rightKeys = new LinkedList<String>();
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter the Field to select from right Dataset (Comma seperated numbers): ");
				String[] tmpArray = tmp.trim().split(",");
				for(String tmp2:tmpArray)
				{
					String tempKey = null;
					long left = Long.parseLong(tmp2);
					cnt = 1;
					for(String dim:rightFields.keySet())
					{
						if(cnt==left)
						{
							tempKey = dim;
							break;
						}
						cnt++;
					}
					if(tempKey==null)
					{
						rightKeys.clear();
						break;
					}
					rightKeys.add(tempKey);
				}
			}catch(Throwable t)
			{				
				rightKeys.clear();
			}
			if(!rightKeys.isEmpty())
				break;
		}

		String newDataSetName = null;
		System.out.println("\n");
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Enter New Dataset Name: ");
				if(tmp!=null && !tmp.trim().isEmpty() && !tmp.trim().equalsIgnoreCase(leftDataSet) && !tmp.trim().equalsIgnoreCase(rightDataSet) && !tmp.trim().endsWith("__c"))
					newDataSetName = tmp.trim();
				if(newDataSetName!=null)
					break;
			}catch(Exception me)
			{				
				newDataSetName = null;
			}
		}
		
//		String newDataSetAlias = DatasetUtils.replaceSpecialCharacters(newDataSetName);
		String newDataSetAlias = ExternalFileSchema.createDevName(newDataSetName, "Dataset", 1, false);

		String workflowName = leftDataSet+"2"+rightDataSet;//"SalesEdgeEltWorkflow"
		
		File dataflowFile = new File(workflowName+".json");

		LinkedHashMap wfdef = new LinkedHashMap();
		wfdef.put(leftDataSet, EdgemartNode.getNode(leftDataSet));
		wfdef.put(rightDataSet, EdgemartNode.getNode(rightDataSet));
		wfdef.put(leftDataSet+"2"+rightDataSet,AugmentNode.getNode(leftDataSet, rightDataSet, leftKey, rightKey, rightKeys.toArray(new String[0])));
		wfdef.put("Register"+leftDataSet+"2"+rightDataSet,RegisterNode.getNode(leftDataSet+"2"+rightDataSet, newDataSetAlias, newDataSetName, null));
		System.out.println("\n");
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(wfdef));
			mapper.writerWithDefaultPrettyPrinter().writeValue(dataflowFile, wfdef);
			System.out.println("\ndataflow succesfully generated and saved in file {"+dataflowFile+"}");
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {		 
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("\n");

		boolean uploadAndStart = false;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Upload and start new workflow? (Yes/No): ");
				if(tmp!=null && (tmp.equalsIgnoreCase("yes") || tmp.equalsIgnoreCase("y")))
				{
					uploadAndStart = true;
					break;
				}else if(tmp!=null && (tmp.equalsIgnoreCase("no") || tmp.equalsIgnoreCase("n")))
				{
					uploadAndStart = false;
					break;
				}
			}catch(Throwable me)
			{				
				uploadAndStart = false;
			}
		}
		
		if(uploadAndStart)
		{
			DataFlowUtil.uploadAndStartDataFlow(partnerConnection, wfdef, "SalesEdgeEltWorkflow");
		}
		
	}
	
	public static Map<String,String> removeDateParts(Map<String,String> dims)
	{
		LinkedHashMap<String,String> out = new LinkedHashMap<String,String>();
		for(String dim:dims.keySet())
		{
			if(dim.endsWith("_Day") || dim.endsWith("_Month") || dim.endsWith("_Year") || dim.endsWith("_Quarter") || dim.endsWith("_Week") || dim.endsWith("_Hour")|| dim.endsWith("_Minute")|| dim.endsWith("_Second")|| dim.endsWith("_Month_Fiscal")|| dim.endsWith("_Year_Fiscal")|| dim.endsWith("_Quarter_Fiscal") || dim.endsWith("_Week_Fiscal"))
			{
				continue;
			}
			if(dim.endsWith("_sec_epoch") || dim.endsWith("_day_epoch"))
			{
				continue;
			}
			out.put(dim, dims.get(dim));
		}
		return out;	
	}
	
}
