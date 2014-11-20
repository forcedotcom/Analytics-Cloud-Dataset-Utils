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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.dataset.loader.EbinFormatWriter;
import com.sforce.dataset.loader.file.schema.ExternalFileSchema;
import com.sforce.dataset.util.CharsetChecker;
import com.sforce.dataset.util.DatasetAugmenter;
import com.sforce.dataset.util.DatasetDownloader;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.SfdcExtracter;
import com.sforce.dataset.util.XmdUploader;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class DatasetUtilMain {
	
	public static final String defaultEndpoint = "https://login.salesforce.com/services/Soap/u/31.0";

	public static void main(String[] args) {
		
		if(!printlneula())
		{
			System.err.println("You do not have permission to use this jar. Please delete it from this computer");
			System.exit(-1);
		}

		String dataset = null;
		String datasetLabel = null;
		String app = null;
		String username = null;
		String password = null;
		String token = null;
		String sessionId = null;
		String endpoint = null;
		String action = null;
		String inputFile = null;
//		String jsonConfig = null;
		String rootObject = null;
		String fileEncoding = "UTF-8";
		Charset fileCharset = null;
		String uploadFormat = "binary";
		String Operation = "Overwrite";
		int rowLimit = 1000;
		boolean useBulkAPI = false;
		CodingErrorAction codingErrorAction = CodingErrorAction.REPORT;
				
		if (args.length > 2) 
		{
			for (int i=1; i< args.length; i=i+2){
				if(args[i-1].equalsIgnoreCase("--u"))
				{
					username = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--p"))
				{
					password = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--sessionId"))
				{
					sessionId = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--token"))
				{
					token = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--endpoint"))
				{
					endpoint = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--action"))
				{
					action = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--inputFile"))
				{
					inputFile = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--dataset"))
				{
					dataset = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--app"))
				{
					app = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--useBulkAPI"))
				{
					if(args[i]!=null && args[i].trim().equalsIgnoreCase("true"))
						useBulkAPI = true;
				}
				else if(args[i-1].equalsIgnoreCase("--uploadFormat"))
				{
					if(args[i]!=null && args[i].trim().equalsIgnoreCase("csv"))
						uploadFormat = "csv";
				}
				else if(args[i-1].equalsIgnoreCase("--rowLimit"))
				{
					if(args[i]!=null && !args[i].trim().isEmpty())
						rowLimit = (new BigDecimal(args[i].trim())).intValue();
				}
//				else if(args[i-1].equalsIgnoreCase("--json"))
//				{
//					jsonConfig = args[i];
//				}
				else if(args[i-1].equalsIgnoreCase("--rootObject"))
				{
					rootObject = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--fileEncoding"))
				{
					fileEncoding = args[i];
				}
				else if(args[i-1].equalsIgnoreCase("--codingErrorAction"))
				{
					if(args[i]!=null)
					{
						if(args[i].equalsIgnoreCase("IGNORE"))
						{
							codingErrorAction = CodingErrorAction.IGNORE;
						}else if(args[i].equalsIgnoreCase("REPORT"))
						{
							codingErrorAction = CodingErrorAction.REPORT;
						}else if(args[i].equalsIgnoreCase("REPLACE"))
						{
							codingErrorAction = CodingErrorAction.REPLACE;
						}
					}
				}else
				{
					printUsage();
					System.err.println("\nERROR: Invalid argument: "+args[i-1]);
					System.exit(-1);
				}
	
			}//end for
		}else
		{
			printUsage();
			System.exit(-1);
		}

		if (action == null) 
		{
				printUsage();
				System.err.println("\nERROR: action must be specified");
				System.exit(-1);
		}
			
		if(sessionId != null)
		{
			if(endpoint == null)
			{
				printUsage();
				System.err.println("\nERROR: endpoint must be specified when sessionId is specified");
				System.exit(-1);
			}
				
				if(endpoint.equals(defaultEndpoint))
				{
					printUsage();
					System.err.println("\nERROR: endpoint must be the actual serviceURL and not the login url");
					System.exit(-1);
				}
		}else
		{
				if(endpoint == null)
				{
					endpoint = defaultEndpoint;
				}
		}
		
		if(endpoint!=null)
		{
			try 
			{
				if(endpoint.equalsIgnoreCase("PROD") || endpoint.equalsIgnoreCase("PRODUCTION"))
				{
					endpoint = defaultEndpoint;
				}else if(endpoint.equalsIgnoreCase("TEST") || endpoint.equalsIgnoreCase("SANDBOX"))
				{
					endpoint = defaultEndpoint.replace("login", "test");
				}
				
				URI uri = new URI(endpoint);
				String scheme = uri.getScheme();
				String host = uri.getHost();
				if(!scheme.equalsIgnoreCase("https"))
				{
					if(!host.toLowerCase().endsWith("internal.salesforce.com") && !host.toLowerCase().endsWith("localhost"))
					{
						System.err.println("\nERROR: UNSUPPORTED_CLIENT: HTTPS Required in endpoint");
						System.exit(-1);
					}
				}
				
				if(uri.getPath() == null || uri.getPath().isEmpty())
				{
					uri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), "/services/Soap/u/31.0", uri.getQuery(), uri.getFragment()); 
				}
				endpoint = uri.toString();
			} catch (URISyntaxException e) {
				e.printStackTrace();
				System.err.println("\nERROR: endpoint is not a valid URL");
				System.exit(-1);
			}
		}
		
		if (inputFile!=null) 
		{
			File temp = new File(inputFile);
			if(!temp.exists() && !temp.canRead())
			{
				System.err.println("\nERROR: inputFile {"+temp+"} not found");
				System.exit(-1);
			}
			
			String ext = FilenameUtils.getExtension(temp.getName());
			if(ext == null || !(ext.equalsIgnoreCase("csv") || ext.equalsIgnoreCase("bin") || ext.equalsIgnoreCase("gz")  || ext.equalsIgnoreCase("json")))
			{
				System.err.println("\nERROR: inputFile does not have valid extension");
				System.exit(-1);
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
								System.err.println("\nERROR: inputFile {"+temp+"} in not valid");
								System.exit(-1);
							}
						} catch (FileNotFoundException e) {
							e.printStackTrace();
							System.err.println("\nERROR: inputFile {"+temp+"} not found");
							System.exit(-1);
						} catch (IOException e) {
							e.printStackTrace();
							System.err.println("\nERROR: inputFile {"+temp+"} in not valid");
							System.exit(-1);
						}
					
						if(!EbinFormatWriter.isValidBin(binHeader))
						{
							if(ext.equalsIgnoreCase("bin"))
							{
								System.err.println("\nERROR: inputFile {"+temp+"} in not valid binary file");
								System.exit(-1);
							}else
							{
								uploadFormat = "csv"; //Assume the user is uploading a .gz csv
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
					System.err.println("\nERROR: inputFile {"+temp+"} is not valid json, Error: " + t.getMessage());
					System.exit(-1);
				}
			}
					
		}

		if(dataset!=null)
		{
			datasetLabel = dataset;
			dataset = ExternalFileSchema.createDevName(dataset, "Dataset", 1);
			if(!dataset.equals(datasetLabel))
			{
				System.err.println("\n Warning: dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
				System.err.println("\n changing dataset name to: {"+dataset+"}");
			}
		}
		
		try
		{
			fileCharset = Charset.forName(fileEncoding);		
		} catch (Throwable  e) {
			e.printStackTrace();
			System.err.println("\nERROR: Invalid fileEncoding {"+fileEncoding+"}");
			System.exit(-1);
		}
			
			if(action.equalsIgnoreCase("load"))
			{
				if (inputFile==null) 
				{
					printUsage();
					System.err.println("\nERROR: inputFile must be specified");
					System.exit(-1);
				}
				
				if(dataset!=null && ((username == null) && (sessionId==null)))
				{
					printUsage();
					System.err.println("\nERROR: username must be specified");
					System.exit(-1);
				}
				
				if(sessionId==null)
				{
					if((username != null) && dataset==null)
					{
						printUsage();
						System.err.println("\nERROR: dataset must be specified");
						System.exit(-1);
					}
					
					if(dataset!=null && (password == null || password.isEmpty()))
					{
						try {
							password = DatasetUtils.readPasswordFromConsole("Enter password: ");
						} catch (IOException e) {
						}						
					}
					
					if(dataset!=null && (password == null || password.isEmpty()))
					{
						printUsage();
						System.err.println("\nERROR: password must be specified");
						System.exit(-1);
					}
				}else
				{
					if(dataset==null)
					{
						printUsage();
						System.err.println("\nERROR: dataset must be specified");
						System.exit(-1);
					}
				}
				
				PartnerConnection partnerConnection = null;
				if(username!=null || sessionId != null)
				{
					try {
						partnerConnection  = DatasetUtils.login(0, username, password, token, endpoint, sessionId);
					} catch (ConnectionException e) {
						e.printStackTrace();
						System.exit(-1);
					}
				}
				
				try {
					
					DatasetLoader.uploadDataset(inputFile, uploadFormat, codingErrorAction, fileCharset, dataset, app, datasetLabel, Operation, useBulkAPI, partnerConnection);
//						DatasetLoader.uploadDataset(inputFile, dataset, app, username, password, endpoint, token, sessionId, useBulkAPI, uploadFormat, codingErrorAction, fileCharset);
				} catch (Exception e) {
						System.err.println();
						e.printStackTrace();
						System.exit(-1);
				}
			} else if(action.equalsIgnoreCase("detectEncoding"))
			{
				if (inputFile==null) 
				{
					printUsage();
					System.err.println("\nERROR: inputFile must be specified");
					System.exit(-1);
				}
				
				try {
					CharsetChecker.detectCharset(new File(inputFile));
				} catch (Exception e) {
						e.printStackTrace();
						System.exit(-1);
				}
			}else if(action.equalsIgnoreCase("uploadxmd"))
				{
					if (inputFile==null) 
					{
						printUsage();
						System.err.println("\nERROR: inputFile must be specified");
						System.exit(-1);
					}
					
					if(dataset==null)
					{
						printUsage();
						System.err.println("\nERROR: dataset must be specified");
						System.exit(-1);
					}else
					{
						
					}

					if(sessionId==null)
					{
						if(username == null)
						{
							printUsage();
							System.err.println("\nERROR: username must be specified");
							System.exit(-1);
						}
						
						if(password == null || password.isEmpty())
						{
							try {
								password = DatasetUtils.readPasswordFromConsole("Enter password: ");
							} catch (IOException e) {
							}						
						}
						
						if(password == null || password.isEmpty())
						{
							printUsage();
							System.err.println("\nERROR: password must be specified");
							System.exit(-1);
						}
					}

					try {
						XmdUploader.uploadXmd(inputFile, dataset, username, password, endpoint, token, sessionId);
					} catch (Exception e) {
							e.printStackTrace();
							System.exit(-1);
					}
			} else if(action.equalsIgnoreCase("downloadxmd"))
			{
				if (dataset==null) 
				{
					printUsage();
					System.err.println("\nERROR: dataset alias must be specified");
					System.exit(-1);
				}

				if(sessionId==null)
				{
					if(username == null)
					{
						printUsage();
						System.err.println("\nERROR: username must be specified");
						System.exit(-1);
					}
					
					if(password == null || password.isEmpty())
					{
						try {
							password = DatasetUtils.readPasswordFromConsole("Enter password: ");
						} catch (IOException e) {
						}						
					}
					
					if(password == null || password.isEmpty())
					{
						printUsage();
						System.err.println("\nERROR: password must be specified");
						System.exit(-1);
					}
				}
				
				try {
					DatasetDownloader.downloadEM(dataset, username, password, endpoint, token, sessionId);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}else if(action.equalsIgnoreCase("augment"))
			{
				if(sessionId==null)
				{
					if(username == null)
					{
						printUsage();
						System.err.println("\nERROR: username must be specified");
						System.exit(-1);
					}
					
					if(password == null || password.isEmpty())
					{
						try {
							password = DatasetUtils.readPasswordFromConsole("Enter password: ");
						} catch (IOException e) {
						}						
					}
					
					if(password == null || password.isEmpty())
					{
						printUsage();
						System.err.println("\nERROR: password must be specified");
						System.exit(-1);
					}
				}
				
				try {
					DatasetAugmenter.augmentEM(username, password, endpoint, token, sessionId);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}else if(action.equalsIgnoreCase("extract"))
			{
				if(rootObject==null)
				{
					printUsage();
					System.err.println("\nERROR: rootObject must be specified");
					System.exit(-1);
				}

//				if(dataset==null)
//				{
//					System.err.println("\nERROR: dataset must be specified");
//					printUsage();
//					System.exit(-1);
//				}
				
				if(sessionId==null)
				{
					if(username == null)
					{
						printUsage();
						System.err.println("\nERROR: username must be specified");
						System.exit(-1);
					}
					
					if(password == null || password.isEmpty())
					{
						try {
							password = DatasetUtils.readPasswordFromConsole("Enter password: ");
						} catch (IOException e) {
						}						
					}
					
					if(password == null || password.isEmpty())
					{
						printUsage();
						System.err.println("\nERROR: password must be specified");
						System.exit(-1);
					}
				}
				
				try{
					SfdcExtracter.extract(rootObject,dataset, username, password, token, endpoint, sessionId, rowLimit);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}else
			{
				printUsage();
				System.err.println("\nERROR: Invalid action {"+action+"}");
				System.exit(-1);
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
			
}
