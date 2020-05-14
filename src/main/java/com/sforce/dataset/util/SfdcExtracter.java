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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.flow.node.RegisterNode;
import com.sforce.dataset.flow.node.SfdcDigestNode;
import com.sforce.dataset.loader.file.schema.ext.FieldType;
import com.sforce.soap.partner.PartnerConnection;

public class SfdcExtracter {

	public static final NumberFormat nf = NumberFormat.getIntegerInstance();

	@SuppressWarnings("rawtypes")
	public static void extract(String rootSObject,String datasetAlias, PartnerConnection partnerConnection, int rowLimit) throws Exception
	{
		if(SfdcUtils.excludedObjects.contains(rootSObject))
		{
			System.out.println("Error: Object {"+rootSObject+"} not supported");
			return;
		}

		Map<String,String> selectedObjectList = new LinkedHashMap<String,String>();

		Map<String,String> objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+rootSObject+"\\b"), false);
		System.out.println("\n");
		if(objectList==null || objectList.size()==0)
		{
			System.out.println("Error: Object {"+rootSObject+"} not found");
			return;
		}
		if( objectList.size()>1)
		{
			System.out.println("Error: More than one Object found {"+objectList.keySet()+"}");
			return;
		}
		selectedObjectList.putAll(objectList);
		
		while(true)
		{
			Map<String, String> tempList = SfdcUtils.getRelatedObjectList(partnerConnection, rootSObject, objectList.get(rootSObject), false);
			objectList.putAll(tempList);		
			int cnt=1;
		    DecimalFormat df = new DecimalFormat("00");
		    df.setMinimumIntegerDigits(2);
	
			for(String alias:objectList.keySet())
			{
				if(cnt==1)
					System.out.println("Found {"+objectList.size()+"} Objects...");
				if(selectedObjectList.containsKey(alias))
					System.out.print("X");
				else
					System.out.print(" ");
				System.out.println(DatasetUtils.padLeft(cnt+"",3)+". "+alias+" ("+objectList.get(alias)+")");
				cnt++;
			}
			System.out.println("\n");
	
			rootSObject = null;
			while(true)
			{
				try
				{
					String tmp = DatasetUtils.readInputFromConsole("Enter Object number to Select (0 = done): ");
					long left = Long.parseLong(tmp.trim());
					if(left==0)
						break;
					cnt = 1;
					for(String alias:objectList.keySet())
					{
						if(cnt==left)
						{
							rootSObject = alias;
							break;
						}
						cnt++;
					}
					if(rootSObject!=null)
						break;
				}catch(Throwable me)
				{				
					rootSObject = null;
				}
			}
	
			if(rootSObject != null)
				selectedObjectList.put(rootSObject, objectList.get(rootSObject));
			else
				break;
		}


		String workflowName = "SalesEdgeEltWorkflow";
		File dataDir = new File(workflowName);
		try
		{
			FileUtils.forceMkdir(dataDir);
		}catch(Throwable t)
		{
			t.printStackTrace();
		}
		LinkedHashMap<String,List<FieldType>> objectFieldMap = previewData(selectedObjectList, partnerConnection, dataDir, rowLimit);
		System.out.println("\n");
		boolean generateWorkflow = false;
		while(true)
		{
			try
			{
				String tmp = DatasetUtils.readInputFromConsole("Generate workflow? (Yes/No): ");
				if(tmp!=null && (tmp.equalsIgnoreCase("yes") || tmp.equalsIgnoreCase("y")))
				{
					generateWorkflow = true;
					break;
				}else if(tmp!=null && (tmp.equalsIgnoreCase("no") || tmp.equalsIgnoreCase("n")))
				{
					generateWorkflow = false;
					break;
				}
			}catch(Throwable me)
			{				
				generateWorkflow = false;
			}
		}
		
		if(!generateWorkflow)
			return;

		for(String objectName:objectFieldMap.keySet())
		{
				boolean canWrite = false;
				while(!canWrite)
				{
					File csvFile = new File(dataDir,objectName+".csv");	
					try					
					{
						if(csvFile.exists())
						{
							BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(csvFile,true));
							IOUtils.closeQuietly(bos);
							bos = null;
						}
						canWrite = true;
					}catch(Throwable t)
					{	
//						System.out.println(t.getMessage());
						canWrite = false;
						DatasetUtils.readInputFromConsole("file {"+csvFile+"} is open in excel please close it first, press enter when done: ");
					}
				}
		}
		

		File dataflowFile = new File(dataDir,workflowName+".json");
		LinkedHashMap wfdef = createWF(selectedObjectList, partnerConnection, dataDir,objectFieldMap);
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(dataflowFile, wfdef);
		System.out.println("\n");
		System.out.println("\nDataflow succesfully generated and saved in file {"+dataflowFile+"} File Size {"+nf.format(dataflowFile.length())+"}");
		System.out.println("\n");
		if(dataflowFile.length()<(100*1024))
		{
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
				DataFlowUtil.uploadAndStartDataFlow(partnerConnection, wfdef, workflowName);
			}
		}else
		{
			System.out.println("The Dataflow file is > 100K consider removing fields and upload the file manually");
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static LinkedHashMap createWF(Map<String,String> selectedObjectList, PartnerConnection partnerConnection, File dataDir, LinkedHashMap objectFieldMap) throws Exception
	{
		LinkedHashMap wfdef = new LinkedHashMap();
		if(objectFieldMap==null || objectFieldMap.isEmpty())
		{
			objectFieldMap = previewData(selectedObjectList, partnerConnection, dataDir, 0);
		}

		for(String alias:selectedObjectList.keySet())
		{	
			if(SfdcUtils.excludedObjects.contains(selectedObjectList.get(alias)))
			{
				System.out.println("Skipping object {"+selectedObjectList.get(alias)+"}");
				continue;
			}
			
			if(!wfdef.containsKey(selectedObjectList.get(alias)))
			{
				List<Map<String,String>> flds = new LinkedList<Map<String,String>>();
//				List<FieldType> fields = SfdcUtils.getFieldList(selectedObjectList.get(alias), partnerConnection, false);
				List<FieldType> fields = (List<FieldType>) objectFieldMap.get(selectedObjectList.get(alias));
				File csvFile = new File(dataDir,selectedObjectList.get(alias)+".csv");
				List<String> userFields = new ArrayList<String>();
				if(csvFile.exists() && csvFile.canRead())
				{
					CSVReader reader = null;
						try 
						{
							 reader = new CSVReader(new FileInputStream(csvFile),null , new char[]{','});

//							reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(csvFile), false), DatasetUtils.utf8Decoder(null, null)), CsvPreference.STANDARD_PREFERENCE);
							ArrayList<String> header = reader.nextRecord();
							userFields = header;
//							reader = new CsvReader(new InputStreamReader(new BOMInputStream(new FileInputStream(csvFile), false), DatasetUtils.utf8Decoder(null, null)));
//							if(reader.readHeaders())
//							{
//							String[] header = reader.getHeaders();
//							userFields = Arrays.asList(header);
//							if(reader!=null)
//							{
//								try {
//									reader.close();
//								} catch (Throwable e) {
//								}
//								reader = null;
//							}
//							}
						}catch(Throwable t){t.printStackTrace();}
						finally{
							if(reader!=null)
							{
								try {
									reader.finalise();
								} catch (Throwable e) {
								}
								reader = null;
							}
						}				
				}

				LinkedHashMap<String, String> labels = new LinkedHashMap<String, String>();				
				for(FieldType fld:fields)
				{
					if(userFields.isEmpty() || userFields.contains(fld.getName()))
					{
						if(labels.containsKey(fld.getLabel()))
						{
							System.out.println("field {"+fld.getName()+"} has duplicate label matching field {"+labels.get(fld.getLabel())+"}");
//							continue;
						}
						labels.put(fld.getLabel(), fld.getName());
						Map<String,String> temp = new LinkedHashMap<String,String>();
						temp.put("name", fld.getName());
						temp.put("type", fld.getType());
						flds.add(temp);
					}else
					{
						System.out.println("user has skipped field:"+selectedObjectList.get(alias)+"."+fld.getName());
					}
				}
//				SfdcUtils.read(partnerConnection, selectedObjectList.get(alias), fields, 1000,dataDir);
				wfdef.put(selectedObjectList.get(alias), SfdcDigestNode.getNode(selectedObjectList.get(alias), flds));
				String datasetAlias = "SF_"+selectedObjectList.get(alias);
				if(datasetAlias.endsWith("__c"))
					datasetAlias = datasetAlias.replace("__c", "_c");
				wfdef.put("Register"+selectedObjectList.get(alias),RegisterNode.getNode(selectedObjectList.get(alias), datasetAlias, datasetAlias, null));
			}
		}
		return wfdef;
		//	wfdef.put(leftDataSet+"2"+rightDataSet,AugmentNode.getNode(leftDataSet, rightDataSet, leftKey, rightKey, rightKeys.toArray(new String[0])));
		//	wfdef.put("Register"+leftDataSet+"2"+rightDataSet,RegisterNode.getNode(leftDataSet+"2"+rightDataSet, newDataSetAlias, newDataSetName, null));
		//	System.out.println("\n");
	}
	
	public static LinkedHashMap<String,List<FieldType>> previewData(Map<String,String> selectedObjectList, PartnerConnection partnerConnection, File dataDir, int rowLimit) throws Exception
	{
		LinkedHashMap<String,List<FieldType>> wfdef = new LinkedHashMap<String,List<FieldType>>();
		for(String alias:selectedObjectList.keySet())
		{	
			if(!wfdef.containsKey(selectedObjectList.get(alias)))
			{
				List<FieldType> fields = SfdcUtils.getFieldList(selectedObjectList.get(alias), partnerConnection, false);
				wfdef.put(selectedObjectList.get(alias), fields);
				try
				{
					SfdcUtils.read(partnerConnection, selectedObjectList.get(alias), fields, rowLimit,dataDir);
				}catch(Throwable t)
				{
					t.printStackTrace();
				}
			}
		}
		return wfdef;
	}

	
}
