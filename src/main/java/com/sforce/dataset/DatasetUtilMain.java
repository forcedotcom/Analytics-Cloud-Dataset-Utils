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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.flow.monitor.DataFlowMonitorUtil;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.dataset.loader.EbinFormatWriter;
import com.sforce.dataset.loader.file.listener.FileListener;
import com.sforce.dataset.loader.file.listener.FileListenerThread;
import com.sforce.dataset.loader.file.listener.FileListenerUtil;
import com.sforce.dataset.loader.file.schema.ExternalFileSchema;
import com.sforce.dataset.util.CharsetChecker;
import com.sforce.dataset.util.DatasetAugmenter;
import com.sforce.dataset.util.DatasetDownloader;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.SfdcExtracter;
import com.sforce.dataset.util.SfdcUtils;
import com.sforce.dataset.util.XmdUploader;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

@SuppressWarnings("deprecation")
public class DatasetUtilMain {
	
	public static final String defaultEndpoint = "https://login.salesforce.com/services/Soap/u/32.0";
	public static final String defaultTestEndpoint = "https://test.salesforce.com/services/Soap/u/32.0";
	
	public static final String[] validActions = {"load", "extract", "augment", "downloadXMD", "uploadXMD", "detectEncoding", "downloadErrorFile"};

	public static void main(String[] args) {
		
		if(!printlneula())
		{
			System.out.println("You do not have permission to use this jar. Please delete it from this computer");
			System.exit(-1);
		}

		DatasetUtilParams params = new DatasetUtilParams();
		String action = null;
				
		if (args.length > 2) 
		{
			for (int i=1; i< args.length; i=i+2){
				if(args[i-1].equalsIgnoreCase("--help") || args[i-1].equalsIgnoreCase("-help") || args[i-1].equalsIgnoreCase("help"))
				{
					printUsage();
				}
				else if(args[i-1].equalsIgnoreCase("--u"))
				{
					params.username = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--p"))
				{
					params.password = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--sessionId"))
				{
					params.sessionId = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--token"))
				{
					params.token = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--endpoint"))
				{
					params.endpoint = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--action"))
				{
					action = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--debug"))
				{
					params.debug = true;
				}
				else if(args[i-1].equalsIgnoreCase("--inputFile"))
				{
					String tmp = args[i];
					if(tmp!=null)
					{
					   File tempFile = new File (tmp);
					   if(tempFile.exists())
					   {
						   params.inputFile =   tempFile.toString();
					   }else
					   {
						   System.out.println("File {"+args[i]+"} does not exist");
						   System.exit(-1);
					   }
					}
				}
				else if(args[i-1].equalsIgnoreCase("--dataset"))
				{
					params.dataset = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--app"))
				{
					params.app = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--useBulkAPI"))
				{
					if(args[i]!=null && args[i].trim().equalsIgnoreCase("true"))
						params.useBulkAPI = true;
				}
				else if(args[i-1].equalsIgnoreCase("--uploadFormat"))
				{
					if(args[i]!=null && args[i].trim().equalsIgnoreCase("csv"))
						params.uploadFormat = "csv";
					else if(args[i]!=null && args[i].trim().equalsIgnoreCase("binary"))
						params.uploadFormat = "binary";
				}
				else if(args[i-1].equalsIgnoreCase("--rowLimit"))
				{
					if(args[i]!=null && !args[i].trim().isEmpty())
						params.rowLimit = (new BigDecimal(args[i].trim())).intValue();
				}
//				else if(args[i-1].equalsIgnoreCase("--json"))
//				{
//					jsonConfig = args[i];
//				}
				else if(args[i-1].equalsIgnoreCase("--rootObject"))
				{
					params.rootObject = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--createNewDateParts"))
				{
					if(args[i]!=null && args[i].trim().equalsIgnoreCase("true"))
						DatasetUtilConstants.createNewDateParts = true;
				}
				else if(args[i-1].equalsIgnoreCase("--fileEncoding"))
				{
					params.fileEncoding = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--codingErrorAction"))
				{
					if(args[i]!=null)
					{
						if(args[i].equalsIgnoreCase("IGNORE"))
						{
							params.codingErrorAction = CodingErrorAction.IGNORE;
						}else if(args[i].equalsIgnoreCase("REPORT"))
						{
							params.codingErrorAction = CodingErrorAction.REPORT;
						}else if(args[i].equalsIgnoreCase("REPLACE"))
						{
							params.codingErrorAction = CodingErrorAction.REPLACE;
						}
					}
				}else
				{
					printUsage();
					System.out.println("\nERROR: Invalid argument: "+args[i-1]);
					System.exit(-1);
				}
			}//end for
				if(params.endpoint == null || params.endpoint.isEmpty())
				{
						params.endpoint = defaultEndpoint;
				}
		}
		
		printBanner();


		if(params.sessionId==null)
		{
			if(params.username == null || params.username.trim().isEmpty())
			{
				params.username = getInputFromUser("Enter salesforce username: ", true, false);						
			}
			
			if(params.username.equals("-1"))
			{
				params.sessionId = getInputFromUser("Enter salesforce sessionId: ", true, false);
				params.username = null;
				params.password = null;
			}else
			{
				if(params.password == null || params.password.trim().isEmpty())
				{
					params.password = getInputFromUser("Enter salesforce password: ", true, true);						
				}
			}
		}
		
		
		if(params.sessionId != null && !params.sessionId.isEmpty())
		{
			while(params.endpoint == null || params.endpoint.trim().isEmpty())
			{
				params.endpoint = getInputFromUser("Enter salesforce instance url: ", true, false);
				if(params.endpoint==null || params.endpoint.trim().isEmpty())
					System.out.println("\nERROR: endpoint must be specified when sessionId is specified");
			}
				
			while(params.endpoint.toLowerCase().contains("login.salesforce.com") || params.endpoint.toLowerCase().contains("test.salesforce.com") || params.endpoint.toLowerCase().contains("test") || params.endpoint.toLowerCase().contains("prod") || params.endpoint.toLowerCase().contains("sandbox"))
			{
				System.out.println("\nERROR: endpoint must be the actual serviceURL and not the login url");
				params.endpoint = getInputFromUser("Enter salesforce instance url: ", true, false);
			}
		}else
		{
			if(params.endpoint == null || params.endpoint.isEmpty())
			{
				params.endpoint = getInputFromUser("Enter salesforce instance url (default=prod): ", false, false);
				if(params.endpoint == null || params.endpoint.trim().isEmpty())
				{
					params.endpoint = defaultEndpoint;
				}
			}
		}
		
			try 
			{
				if(params.endpoint.equalsIgnoreCase("PROD") || params.endpoint.equalsIgnoreCase("PRODUCTION"))
				{
					params.endpoint = defaultEndpoint;
				}else if(params.endpoint.equalsIgnoreCase("TEST") || params.endpoint.equalsIgnoreCase("SANDBOX"))
				{
					params.endpoint = defaultEndpoint.replace("login", "test");
				}
				
				URL uri = new URL(params.endpoint);
				String protocol = uri.getProtocol();
				String host = uri.getHost();
				if(protocol == null || !protocol.equalsIgnoreCase("https"))
				{
					if(host == null || !(host.toLowerCase().endsWith("internal.salesforce.com") || host.toLowerCase().endsWith("localhost")))
					{
						System.out.println("\nERROR: Invalid endpoint. UNSUPPORTED_CLIENT: HTTPS Required in endpoint");
						System.exit(-1);
					}
				}
				
				if(uri.getPath() == null || uri.getPath().isEmpty())
				{
					uri = new URL(uri.getProtocol(), uri.getHost(), uri.getPort(), "/services/Soap/u/32.0"); 
				}
				params.endpoint = uri.toString();
			} catch (MalformedURLException e) {
				e.printStackTrace();
				System.out.println("\nERROR: endpoint is not a valid URL");
				System.exit(-1);
			}

		
		PartnerConnection partnerConnection = null;
		if(params.username!=null || params.sessionId != null)
		{
			try {
				partnerConnection  = DatasetUtils.login(0, params.username, params.password, params.token, params.endpoint, params.sessionId, params.debug);
			} catch (ConnectionException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		if(args.length==0 || action == null)
		{
			System.out.println("\n*******************************************************************************");					
			FileListenerUtil.startAllListener(partnerConnection);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			System.out.println("*******************************************************************************\n");	
			System.out.println();
			while(true)
			{
				action = getActionFromUser();
				if(action==null || action.isEmpty())
				{
					System.exit(-1);
				}
				params = new DatasetUtilParams();
				getRequiredParams(action, partnerConnection, params);
				boolean status = doAction(action, partnerConnection, params);
				if(status)
				{
					if(action.equalsIgnoreCase("load"))
						createListener(partnerConnection, params);
				}
			}
		}else
		{
			doAction(action, partnerConnection, params);
		}
		
	}


	public static void printUsage()
	{
		System.out.println("\n*******************************************************************************");					
		System.out.println("Usage:");
		System.out.print("java -jar datasetutil.jar --action load --u userName --p password ");
		System.out.println("--dataset datasetAlias --inputFile inputFile --endpoint endPoint");
		System.out.println("--action  : Use either:load,extract,augment,downloadxmd,uploadxmd,detectEncoding");
		System.out.println("          : Use load for loading csv, augment for augmenting existing dataset");
		System.out.println("--u       : Salesforce.com login");
		System.out.println("--p       : (Optional) Salesforce.com password,if omitted you will be prompted");
		System.out.println("--token   : (Optional) Salesforce.com token");
		System.out.println("--endpoint: (Optional) The salesforce soap api endpoint (test/prod)");
		System.out.println("          : Default: https://login.salesforce.com/services/Soap/u/31.0");
		System.out.println("--dataset : (Optional) the dataset alias. required if action=load");
		System.out.println("--app     : (Optional) the app name for the dataset");
		System.out.println("--inputFile : (Optional) the input csv file. required if action=load");
		System.out.println("--rootObject: (Optional) the root SObject for the extract");
		System.out.println("--rowLimit: (Optional) the number of rows to extract, -1=all, deafult=1000");
		System.out.println("--sessionId : (Optional) the salesforce sessionId. if specified,specify endpoint");
		System.out.println("--fileEncoding : (Optional) the encoding of the inputFile default UTF-8");
		System.out.println("--uploadFormat : (Optional) the whether to upload as binary or csv. default binary");
		System.out.println("--createNewDateParts : (Optional) wether to create new date parts");
//		System.out.println("jsonConfig: (Optional) the dataflow definition json file");
		System.out.println("*******************************************************************************\n");
		System.out.println("Usage Example 1: Upload a csv to a dataset");
		System.out.println("java -jar datasetutil.jar --action load --u pgupta@force.com --p @#@#@# --inputFile Opportunity.csv --dataset test");
		System.out.println("");
		System.out.println("Usage Example 2: Download dataset xmd files");
		System.out.println("java -jar datasetloader.jar --action downloadxmd --u pgupta@force.com --p @#@#@# --dataset test");
		System.out.println("");
		System.out.println("Usage Example 3: Upload user.xmd.json");
		System.out.println("java -jar datasetutil.jar --action uploadxmd --u pgupta@force.com --p @#@#@# --inputFile user.xmd.json --dataset test");
		System.out.println("");
		System.out.println("Usage Example 4: Augment 2 datasets");
		System.out.println("java -jar datasetutil.jar --action augment --u pgupta@force.com --p @#@#@#");
		System.out.println("");
		System.out.println("Usage Example 5: Generate the schema file from CSV");
		System.out.println("java -jar datasetutil.jar --action load --inputFile Opportunity.csv");
		System.out.println("");
		System.out.println("Usage Example 6: Extract salesforce data");
		System.out.println("java -jar datasetutil.jar --action extract --u pgupta@force.com --p @#@#@# --rootObject OpportunityLineItem");
		System.out.println("");
	}
	
	static boolean printlneula()
	{
		try
		{
			String userHome = System.getProperty("user.home");
			File lic = new File(userHome, ".ac.datautils.lic");
			if(!lic.exists())
			{
				System.out.println(eula);
				System.out.println();
				while(true)
				{
					String response = DatasetUtils.readInputFromConsole("Do you agree to the above license agreement (Yes/No): ");
					if(response!=null && (response.equalsIgnoreCase("yes") || response.equalsIgnoreCase("y")))
					{
						FileUtils.writeStringToFile(lic, eula);
						return true;
					}else if(response!=null && (response.equalsIgnoreCase("no") || response.equalsIgnoreCase("n")))
					{
						return false;
					}
				}
			}else
			{
				return true;
			}
		}catch(Throwable t)
		{
			t.printStackTrace();
		}
		return false;
	}
	
	public static String eula = "/*\n" 
 +"* Copyright (c) 2014, salesforce.com, inc.\n"
 +"* All rights reserved.\n"
 +"*\n" 
 +"* Redistribution and use in source and binary forms, with or without modification, are permitted provided\n" 
 +"* that the following conditions are met:\n"
 +"* \n"
 +"*    Redistributions of source code must retain the above copyright notice, this list of conditions and the \n"
 +"*    following disclaimer.\n"
 +"*  \n"
 +"*    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and \n"
 +"*    the following disclaimer in the documentation and/or other materials provided with the distribution. \n"
 +"*    \n"
 +"*    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or \n"
 +"*    promote products derived from this software without specific prior written permission."
 +"* \n"
 +"* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS \"AS IS\" AND ANY EXPRESS OR IMPLIED \n"
 +"* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A \n"
 +"* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR \n"
 +"* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED \n"
 +"* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) \n"
 +"* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING \n"
 +"* NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE \n"
 +"* POSSIBILITY OF SUCH DAMAGE. \n"
 +"*/\n";
	
	public static String getInputFromUser(String message, boolean isRequired, boolean isPassword)
	{
		String input = null;
		while(true)
		{
			try
			{
				if(!isPassword)
					input = DatasetUtils.readInputFromConsole(message);
				else
					input = DatasetUtils.readPasswordFromConsole(message);
				
				if(input==null || input.isEmpty())
				{
					if(!isRequired)
						break;
				}else
				{
					break;
				}
			}catch(Throwable me)
			{				
				input = null;
			}
		}
		return input;
	}
	
	public static String getActionFromUser()
	{
		System.out.println();
		String selectedAction = "load";
			int cnt=1;
		    DecimalFormat df = new DecimalFormat("00");
		    df.setMinimumIntegerDigits(2);
			for(String action:validActions)
			{
				if(cnt==1)
					System.out.println("Available Datasetutil Actions: ");
				System.out.print(" ");
				System.out.println(DatasetUtils.padLeft(cnt+"",3)+". "+action);
				cnt++;
			}
			System.out.println();
	
			while(true)
			{
				try
				{
					String tmp = DatasetUtils.readInputFromConsole("Enter Action number (0  = Exit): ");
					if(tmp==null)
						return null;
					if(tmp.trim().isEmpty())
						continue;
					long choice = Long.parseLong(tmp.trim());
					if(choice==0)
						return null; 
					cnt = 1;
					if(choice>0 && choice <= validActions.length)
					{
						for(String action:validActions)
						{
							if(cnt==choice)
							{
								selectedAction = action;
								return selectedAction;
							}
							cnt++;
						}
					}
				}catch(Throwable me)
				{				
				}
			}
		}
	
	public static boolean doAction(String action, PartnerConnection partnerConnection, DatasetUtilParams params)
	{

		if(action==null)
		{
			printUsage();
			System.out.println("\nERROR: Invalid action {"+action+"}");
			return false;
		}

		if (params.inputFile!=null) 
		{
			   File tempFile = validateInputFile(params.inputFile, action);
			   if(tempFile == null)
			   {
				   System.out.println("Inputfile {"+params.inputFile+"} is not valid");
				   return false;
			   }
		}

		if(params.dataset!=null && !params.dataset.isEmpty())
		{
			params.datasetLabel = params.dataset;
			params.dataset = ExternalFileSchema.createDevName(params.dataset, "Dataset", 1);
			if(!params.dataset.equals(params.datasetLabel))
			{
				System.out.println("\n Warning: dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
				System.out.println("\n changing dataset name to: {"+params.dataset+"}");
			}
		}
		
		Charset fileCharset = null;
		try
		{
			if(params.fileEncoding!=null && !params.fileEncoding.trim().isEmpty())
				fileCharset = Charset.forName(params.fileEncoding);
			else
				fileCharset = Charset.forName("UTF-8");
		} catch (Throwable  e) {
			e.printStackTrace();
			System.out.println("\nERROR: Invalid fileEncoding {"+params.fileEncoding+"}");
			return false;
		}
			

		
			if(action.equalsIgnoreCase("load"))
			{
				if (params.inputFile==null || params.inputFile.isEmpty()) 
				{
					System.out.println("\nERROR: inputFile must be specified");
					return false;
				}
				
				if (params.dataset==null || params.dataset.isEmpty()) 
				{
					System.out.println("\nERROR: dataset name must be specified");
					return false;
				}
				
				try {
					return DatasetLoader.uploadDataset(params.inputFile, params.uploadFormat, params.codingErrorAction,fileCharset, params.dataset, params.app, params.datasetLabel, params.Operation, params.useBulkAPI, partnerConnection, System.out);
				} catch (Exception e) {
						System.out.println();
						e.printStackTrace();
						return false;
				}
			} else if(action.equalsIgnoreCase("detectEncoding"))
			{
				if (params.inputFile==null) 
				{
					System.out.println("\nERROR: inputFile must be specified");
					return false;
				}
				
				try {
					CharsetChecker.detectCharset(new File(params.inputFile));
				} catch (Exception e) {
						e.printStackTrace();
						return false;
				}
			}else if(action.equalsIgnoreCase("uploadxmd"))
				{
					if (params.inputFile==null) 
					{
						System.out.println("\nERROR: inputFile must be specified");
						return false;
					}
					
					if(params.dataset==null)
					{
						System.out.println("\nERROR: dataset must be specified");
						return false;
					}


					try {
						XmdUploader.uploadXmd(params.inputFile, params.dataset, partnerConnection);
					} catch (Exception e) {
							e.printStackTrace();
							return false;
					}
			} else if(action.equalsIgnoreCase("downloadxmd"))
			{
				if (params.dataset==null) 
				{
					System.out.println("\nERROR: dataset alias must be specified");
					return false;
				}
				
				try {
					DatasetDownloader.downloadEM(params.dataset, partnerConnection);
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}else if(action.equalsIgnoreCase("augment"))
			{
				
				try {
					DatasetAugmenter.augmentEM(partnerConnection);
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}else if(action.equalsIgnoreCase("extract"))
			{
				if(params.rootObject==null)
				{
					System.out.println("\nERROR: rootObject must be specified");
					return false;
				}
				
				try{
					SfdcExtracter.extract(params.rootObject,params.dataset, partnerConnection, params.rowLimit);
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}else if(action.equalsIgnoreCase("downloadErrorFile"))
			{
				if (params.dataset==null) 
				{
					System.out.println("\nERROR: dataset alias must be specified");
					return false;
				}

				try {
					DataFlowMonitorUtil.getJobsAndErrorFiles(partnerConnection, params.dataset);
				} catch (Exception e) {
						System.out.println();
						e.printStackTrace();
						return false;
				}
				
			}else
			{
				printUsage();
				System.out.println("\nERROR: Invalid action {"+action+"}");
				return false;
			}
			return true;
	}
	
	
	public static void getRequiredParams(String action,PartnerConnection partnerConnection, DatasetUtilParams params)
	{
		if (action == null || action.trim().isEmpty())
		{
				System.out.println("\nERROR: Invalid action {"+action+"}");
				System.out.println();
				return;
		}else
		if(action.equalsIgnoreCase("load"))
		{
			while (params.inputFile==null || params.inputFile.isEmpty()) 
			{
				String tmp = getInputFromUser("Enter inputFile: ", true, false);
				if(tmp!=null)
				{
					   File tempFile = validateInputFile(tmp, action);
					   if(tempFile !=null)
					   {
						   params.inputFile =   tempFile.toString();
						   break;
					   }
				}else 
				 System.out.println("File {"+tmp+"} not found");
				System.out.println();
			}

			if (params.dataset==null || params.dataset.isEmpty()) 
			{
				params.dataset = getInputFromUser("Enter dataset name: ", true, false);						
			}

			if (params.datasetLabel==null || params.datasetLabel.isEmpty()) 
			{
				params.datasetLabel = getInputFromUser("Enter datasetLabel (Optional): ", false, false);						
			}

			if (params.app==null || params.app.isEmpty()) 
			{
				params.app = getInputFromUser("Enter datasetFolder (Optional): ", false, false);
				if(params.app != null && params.app.isEmpty())
					params.app = null;
			}

			if (params.fileEncoding==null || params.fileEncoding.isEmpty()) 
			{
				while(true)
				{
					params.fileEncoding = getInputFromUser("Enter fileEncoding (default=UTF-8): ", false, false);
					if(params.fileEncoding == null || params.fileEncoding.trim().isEmpty())
						params.fileEncoding = "UTF-8";
					try
					{
						Charset.forName(params.fileEncoding);
						break;
					} catch (Throwable  e) {
					}
					System.out.println("\nERROR: Invalid fileEncoding {"+params.fileEncoding+"}");
					System.out.println();
				}
			}

			while (params.uploadFormat==null || params.uploadFormat.isEmpty()) 
			{
				params.uploadFormat = getInputFromUser("Enter uploadFormat (csv or binary) (default=binary): ", false, false);						
				if(params.uploadFormat == null || params.uploadFormat.trim().isEmpty())
					params.uploadFormat = "binary";
				if(!params.uploadFormat.equalsIgnoreCase("csv") && !params.uploadFormat.equalsIgnoreCase("binary"))
				{
					System.out.println("Invalid upload format {"+params.uploadFormat+"}");
				}else
				{
					break;
				}
				System.out.println();
			}
			
		}else if(action.equalsIgnoreCase("downloadErrorFile"))
		{
			if (params.dataset==null || params.dataset.isEmpty()) 
			{
				params.dataset = getInputFromUser("Enter dataset name: ", true, false);						
			}
			
		} else if(action.equalsIgnoreCase("detectEncoding"))
		{
			while (params.inputFile==null || params.inputFile.isEmpty()) 
			{
				String tmp = getInputFromUser("Enter inputFile: ", true, false);
				if(tmp!=null)
				{
					   File tempFile = validateInputFile(tmp, action);
					   if(tempFile !=null)
					   {
						   params.inputFile =   tempFile.toString();
						   break;
					   }
				} else
					System.out.println("File {"+tmp+"} not found");
				System.out.println();
			}
				
		}else if(action.equalsIgnoreCase("uploadxmd"))
		{
			while (params.inputFile==null || params.inputFile.isEmpty()) 
			{
				String tmp = getInputFromUser("Enter inputFile: ", true, false);
				if(tmp!=null)
				{
				   File tempFile = validateInputFile(tmp, action);
				   if(tempFile !=null)
				   {
					   params.inputFile =   tempFile.toString();
					   break;
				   }
				}else
					System.out.println("File {"+tmp+"} not found");
				System.out.println();
			}

			if (params.dataset==null || params.dataset.isEmpty()) 
			{
				params.dataset = getInputFromUser("Enter dataset name: ", true, false);						
			}
		} else if(action.equalsIgnoreCase("downloadxmd"))
		{
			if (params.dataset==null || params.dataset.isEmpty()) 
			{
				params.dataset = getInputFromUser("Enter dataset name: ", true, false);						
			}
		}else if(action.equalsIgnoreCase("augment"))
		{
				
		}else if(action.equalsIgnoreCase("extract"))
		{
			while (params.rootObject==null || params.rootObject.isEmpty()) 
			{
				String tmp = getInputFromUser("Enter root SObject name for Extract: ", true, false);
				Map<String, String> objectList = null;
				try {
					objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+tmp+"\\b"), false);
				} catch (ConnectionException e) {
				}
				if(objectList==null || objectList.size()==0)
				{
					System.out.println("\nError: Object {"+tmp+"} not found");
					System.out.println();
				}else
				{
					params.rootObject = tmp;
					break;
				}
			}
				
		}else if(action.equalsIgnoreCase("downloadErrorFile"))
		{
			if (params.dataset==null || params.dataset.isEmpty()) 
			{
				params.dataset = getInputFromUser("Enter dataset name: ", true, false);						
			}
		}else
		{
				printUsage();
				System.out.println("\nERROR: Invalid action {"+action+"}");
		}
	}
	
	public static File validateInputFile(String inputFile, String action)
	{
		File temp = null;
		if (inputFile!=null) 
		{
			temp = new File(inputFile);
			if(!temp.exists() && !temp.canRead())
			{
				System.out.println("\nERROR: inputFile {"+temp+"} not found");
				return null;
			}
			
			String ext = FilenameUtils.getExtension(temp.getName());
			if(ext == null || !(ext.equalsIgnoreCase("csv") || ext.equalsIgnoreCase("bin") || ext.equalsIgnoreCase("gz")  || ext.equalsIgnoreCase("json")))
			{
				System.out.println("\nERROR: inputFile does not have valid extension");
				return null;
			}
			
			if(action.equalsIgnoreCase("load"))
			{
					byte[] binHeader = new byte[5];
					if(ext.equalsIgnoreCase("bin") || ext.equalsIgnoreCase("gz"))
					{
						try {
							InputStream fis = new FileInputStream(temp);
							if(ext.equalsIgnoreCase("gz"))
								fis = new GzipCompressorInputStream(new FileInputStream(temp));
							int cnt = fis.read(binHeader);
							if(fis!=null)
							{
								IOUtils.closeQuietly(fis);
							}
							if(cnt<5)
							{
								System.out.println("\nERROR: inputFile {"+temp+"} in not valid");
								return null;
							}
						} catch (FileNotFoundException e) {
							e.printStackTrace();
							System.out.println("\nERROR: inputFile {"+temp+"} not found");
							return null;
						} catch (IOException e) {
							e.printStackTrace();
							System.out.println("\nERROR: inputFile {"+temp+"} in not valid");
							return null;
						}
					
						if(!EbinFormatWriter.isValidBin(binHeader))
						{
							if(ext.equalsIgnoreCase("bin"))
							{
								System.out.println("\nERROR: inputFile {"+temp+"} in not valid binary file");
								return null;
							}
						}
					}
			}
			
			if(ext.equalsIgnoreCase("json"))
			{
				try
				{
					ObjectMapper mapper = new ObjectMapper();	
					mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
					mapper.readValue(temp, Object.class);
				}catch(Throwable t)
				{
					System.out.println("\nERROR: inputFile {"+temp+"} is not valid json, Error: " + t.getMessage());
					return null;
				}
			}
					
		}
		return temp;
		
	}
	

	private static void createListener(PartnerConnection partnerConnection,
			DatasetUtilParams params) {
		String response = getInputFromUser("Do you want to create a FileListener for above file upload (Yes/No): ", false, false);
		if(response!=null && (response.equalsIgnoreCase("Y") || response.equalsIgnoreCase("YES")))
		{
			try {
				FileListener fileListener = new FileListener();
				fileListener.setApp(params.app);
				fileListener.setCea(params.codingErrorAction);
				fileListener.setDataset(params.dataset);
				fileListener.setDatasetLabel(params.datasetLabel);
				fileListener.setFilecharset(params.fileEncoding);
				fileListener.setInputFileDirectory(params.inputFile);
//				fileListener.setInputFilePattern(inputFilePattern);
				fileListener.setOperation(params.Operation);
				fileListener.setUploadFormat(params.uploadFormat);
				fileListener.setUseBulkAPI(params.useBulkAPI);
//				fileListener.setFileAge(fileAge);
//				fileListener.setPollingInterval();
				FileListenerThread.moveInputFile(new File(params.inputFile), System.currentTimeMillis(), true);
				FileListenerUtil.addAndStartListener(fileListener, partnerConnection);
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	
	public static void printBanner()
	{
		for(int i=0;i<5;i++)
			System.out.println();
		System.out.println("\n\t\t****************************************");
		System.out.println("\t\tSalesforce Analytics Cloud Dataset Utils");
		System.out.println("\t\t****************************************\n");					
	}
	

			
}
