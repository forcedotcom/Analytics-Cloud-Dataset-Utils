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
package com.sforce.dataset.loader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.flow.monitor.ThreadContext;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.loader.file.schema.ext.FieldType;
import com.sforce.dataset.loader.file.sort.CsvExternalSort;
import com.sforce.dataset.util.CharsetChecker;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.FileUtilsExt;
import com.sforce.dataset.util.SfdcUtils;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.util.Base64;

/**
 * The Class DatasetLoader.
 */
public class DatasetLoader {
	

	/** The Constant DEFAULT_BUFFER_SIZE. */
	private static final int DEFAULT_BUFFER_SIZE = 8*1024*1024;
	
	/** The Constant EOF. */
	private static final int EOF = -1;
	
	/** The Constant LF. */
	private static final char LF = '\n';
	
	/** The Constant CR. */
	private static final char CR = '\r';
	
	/** The Constant QUOTE. */
	private static final char QUOTE = '"';
	
	/** The Constant COMMA. */
	private static final char COMMA = ',';

	
	/** The Constant filePartsHdr. */
	private static final String[] filePartsHdr = {"InsightsExternalDataId","PartNumber","DataFile"};
	
	/** The Constant nf. */
	public static final NumberFormat nf = NumberFormat.getIntegerInstance();
	
	/** The max num upload threads. */
	private static int MAX_NUM_UPLOAD_THREADS = 3;
	
	/** The Constant logformat. */
	static final SimpleDateFormat logformat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS zzz");
	static
	{
		logformat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
//	PrintStream logger = null;

/**
 * Upload dataset.
 *
 * @param inputCsv the input csv
 * @param inputFileCharset the input file charset
 * @param datasetAlias the dataset alias
 * @param datasetFolder the dataset folder
 * @param datasetLabel the dataset label
 * @param Operation the operation (OverWrite/Upserts/Append)
 * @param fields the list of fields in the csv
 * @param sessionID the sfdc authenticated session id
 * @param serverInstanceURL the server instance url returned from the authentication
 * @param logger the logger
 * @throws DatasetLoaderException the dataset loader exception
 */
	public static void uploadDataset(File inputCsv,
			Charset inputFileCharset, String datasetAlias,
			String datasetFolder,String datasetLabel, String Operation, List<FieldType> fields,
			String sessionID, String serverInstanceURL, PrintStream logger) throws DatasetLoaderException
	{
		//Blackbox
	}

	/**
 * Upload dataset.
 *
 * @param inputFileString the input file string
 * @param uploadFormat the upload format
 * @param codingErrorAction the coding error action
 * @param inputFileCharset the input file charset
 * @param datasetAlias the dataset alias
 * @param datasetFolder the dataset folder
 * @param datasetLabel the dataset label
 * @param Operation the operation
 * @param useBulkAPI the use bulk api
 * @param partnerConnection the partner connection
 * @param logger the logger
 * @param notificationLevel notificationLevel
 * @param notificationEmail notificationEmail
 * @return true, if successful
 * @throws DatasetLoaderException the dataset loader exception
 */
@SuppressWarnings("deprecation")
	public static boolean uploadDataset(String inputFileString,String schemaFileString,
			String uploadFormat, CodingErrorAction codingErrorAction,
			Charset inputFileCharset, String datasetAlias,
			String datasetFolder,String datasetLabel, String Operation, boolean useBulkAPI,
			PartnerConnection partnerConnection,String notificationLevel, String notificationEmail, PrintStream logger) throws DatasetLoaderException
	{
		File archiveDir = null;
		File datasetArchiveDir = null;
		File inputFile = null;
		File schemaFile = null;
		boolean status = true;
		long digestTime = 0L;
		long uploadTime = 0L;
		boolean updateHdrJson = false;        

		ThreadContext tx = ThreadContext.get();
		Session session = tx.getSession();
		
		//we only want a small capacity otherwise the reader thread will runaway
		BlockingQueue<String[]> q = new LinkedBlockingQueue<String[]>(10);  

		
		if(uploadFormat==null||uploadFormat.trim().isEmpty())
			uploadFormat = "binary";
		
		if(codingErrorAction==null)
			codingErrorAction = CodingErrorAction.REPORT;
		

		if(logger==null)
			logger = System.out;
		

		if(inputFileCharset==null)
		{
			Charset tmp = null;
			try 
			{
				inputFile = new File(inputFileString);
				if(inputFile.exists() && inputFile.length()>0)
				{
					tmp = CharsetChecker.detectCharset(inputFile,logger);
				}
			} catch (Exception e) 
			{
			}

			if(tmp!=null)
			{
				inputFileCharset = tmp;
			}else
			{
				inputFileCharset = Charset.forName("UTF-8");
			}
		}
		
		if(Operation == null)
		{
			Operation = "Overwrite";
		}
		
		if(datasetLabel==null || datasetLabel.trim().isEmpty())
		{
			datasetLabel = datasetAlias;
		}
		
		Runtime rt = Runtime.getRuntime();
		long mb = 1024*1024;

		logger.println("\n*******************************************************************************");					
		logger.println("Start Timestamp: "+logformat.format(new Date()));
		logger.println("inputFileString: "+inputFileString);
		logger.println("schemaFileString: "+schemaFileString);
		logger.println("inputFileCharset: "+inputFileCharset);
		logger.println("datasetAlias: "+datasetAlias);
		logger.println("datasetLabel: "+datasetLabel);
		logger.println("datasetFolder: "+datasetFolder);
		logger.println("Operation: "+Operation);
		logger.println("uploadFormat: "+uploadFormat);
		logger.println("notificationLevel: "+notificationLevel);
		logger.println("notificationEmail: "+notificationEmail);
		logger.println("JVM Max memory: "+nf.format(rt.maxMemory()/mb));
		logger.println("JVM Total memory: "+nf.format(rt.totalMemory()/mb));
		logger.println("JVM Free memory: "+nf.format(rt.freeMemory()/mb));
		logger.println("*******************************************************************************\n");					
		
		try {
			
			inputFile = new File(inputFileString);
			if(!inputFile.exists())
			{
				logger.println("Error: File {"+inputFile.getAbsolutePath()+"} not found");
				throw new DatasetLoaderException("File {"+inputFile.getAbsolutePath()+"} not found");
			}

			if(inputFile.length()==0)
			{
				logger.println("Error: File {"+inputFile.getAbsolutePath()+"} is empty");
				throw new DatasetLoaderException("Error: File {"+inputFile.getAbsolutePath()+"} is empty");
			}

			if(schemaFileString != null)
			{
				schemaFile = new File(schemaFileString);
				if(!schemaFile.exists())
				{
					logger.println("Error: File {"+schemaFile.getAbsolutePath()+"} not found");
					throw new DatasetLoaderException("File {"+schemaFile.getAbsolutePath()+"} not found");
				}
	
				if(schemaFile.length()==0)
				{
					logger.println("Error: File {"+schemaFile.getAbsolutePath()+"} is empty");
					throw new DatasetLoaderException("Error: File {"+schemaFile.getAbsolutePath()+"} is empty");
				}
			}
			
			if(datasetAlias==null||datasetAlias.trim().isEmpty())
			{
				throw new DatasetLoaderException("datasetAlias cannot be null");
			}

			String santizedDatasetName = ExternalFileSchema.createDevName(datasetAlias, "Dataset", 1, false);
			if(!datasetAlias.equals(santizedDatasetName))
			{
				logger.println("\n Warning: dataset name can only contain alpha-numeric or '_', must start with alpha, and cannot end in '__c'");
				logger.println("\n changing dataset name to: {"+santizedDatasetName+"}");
				datasetAlias = santizedDatasetName;
			}
			
			if(datasetAlias.length()>80)
				throw new DatasetLoaderException("datasetName {"+datasetAlias+"} should be less than 80 characters");
			

			//Validate access to the API before going any further
			if(!DatasetLoader.checkAPIAccess(partnerConnection, logger))
			{
				logger.println("Error: you do not have access to Analytics Cloud API. Please contact salesforce support");
				throw new DatasetLoaderException("Error: you do not have access to Analytics Cloud API. Please contact salesforce support");
			}
			
			if(session==null)
			{
				session = Session.getCurrentSession(partnerConnection.getUserInfo().getOrganizationId(), datasetAlias, false);
			}
			
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}
			
			if(schemaFile==null)
				schemaFile = ExternalFileSchema.getSchemaFile(inputFile, logger);
			
			ExternalFileSchema schema = null;
			String orgId = partnerConnection.getUserInfo().getOrganizationId();

			
			//If this is incremental, fetch last uploaded json instead of generating a new one
			if(schemaFile == null || !schemaFile.exists() || schemaFile.length()==0)
			{
				if(Operation.equalsIgnoreCase("Append") || (Operation.equalsIgnoreCase("Upsert")) || (Operation.equalsIgnoreCase("Delete")))
				{
					schema = getLastUploadedJson(partnerConnection, datasetAlias, logger);
					if(schemaFile != null && schema !=null)
					{
						ExternalFileSchema.save(schemaFile, schema, logger);
					}
				}
			}

//			if(schema!=null)
//			{
//				if(schemaFile != null && schemaFile.exists() && schemaFile.length()>0)
//				{
//					ExternalFileSchema extSchema = ExternalFileSchema.load(schemaFile, inputFileCharset, logger);
//					ExternalFileSchema.mergeExtendedFields(extSchema, schema, logger);
//				}else if(schemaFile != null)
//				{
//					ExternalFileSchema.save(schemaFile, schema, logger);
//				}
//			}
			
			CsvPreference pref = null;				
			String fileExt = FilenameUtils.getExtension(inputFile.getName());
			boolean isParsable = false;
			if(fileExt != null && (fileExt.equalsIgnoreCase("csv") || fileExt.equalsIgnoreCase("txt") ))
			{
				isParsable = true;
//				if(!fileExt.equalsIgnoreCase("csv"))
//				{
//					char sep = SeparatorGuesser.guessSeparator(inputFile, inputFileCharset, true);
//					if(sep!=0)
//					{
//						pref = new CsvPreference.Builder((char) CsvPreference.STANDARD_PREFERENCE.getQuoteChar(), sep, CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).build();
//					}else
//					{
//						throw new DatasetLoaderException("Failed to determine field Delimiter for file {"+inputFile+"}");
//					}
//				}else
//				{
//					pref = CsvPreference.STANDARD_PREFERENCE;
//				}
			}
				
			
			
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

			
			if(schema==null)
			{
				logger.println("\n*******************************************************************************");					
				if(isParsable)
				{	
					if(schemaFile != null && schemaFile.exists() && schemaFile.length()>0)
						session.setStatus("LOADING SCHEMA");
					else
						session.setStatus("DETECTING SCHEMA");
								
					schema = ExternalFileSchema.init(inputFile, schemaFile, inputFileCharset,logger, orgId);
					if(schema==null)
					{
						logger.println("Failed to parse schema file {"+ ExternalFileSchema.getSchemaFile(inputFile, logger) +"}");
						throw new DatasetLoaderException("Failed to parse schema file {"+ ExternalFileSchema.getSchemaFile(inputFile, logger) +"}");
					}
				}else
				{
					if(schemaFile != null && schemaFile.exists() && schemaFile.length()>0)
						session.setStatus("LOADING SCHEMA");
					schema = ExternalFileSchema.load(inputFile, schemaFile, inputFileCharset,logger);
					if(schema==null)
					{
						logger.println("Failed to load schema file {"+ ExternalFileSchema.getSchemaFile(inputFile, logger) +"}");
						throw new DatasetLoaderException("Failed to load schema file {"+ ExternalFileSchema.getSchemaFile(inputFile, logger) +"}");
					}
				}
				logger.println("*******************************************************************************\n");
			}
			

			if(schema != null)
			{
				if((Operation.equalsIgnoreCase("Upsert") || Operation.equalsIgnoreCase("Delete")) && !ExternalFileSchema.hasUniqueID(schema))
				{
					throw new DatasetLoaderException("Schema File {"+ExternalFileSchema.getSchemaFile(inputFile, logger) +"} must have uniqueId set for atleast one field");
				}

				if(Operation.equalsIgnoreCase("Append") && ExternalFileSchema.hasUniqueID(schema))
				{
					throw new DatasetLoaderException("Schema File {"+ExternalFileSchema.getSchemaFile(inputFile, logger) +"} has a uniqueId set. Choose 'Upsert' operation instead");
				}
				
			   pref = new CsvPreference.Builder((char) CsvPreference.STANDARD_PREFERENCE.getQuoteChar(), schema.getFileFormat().getFieldsDelimitedBy().charAt(0), CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).build();

			}
			
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}


			archiveDir = new File(inputFile.getParent(),"archive");
			try
			{
				FileUtils.forceMkdir(archiveDir);
			}catch(Throwable t)
			{
				t.printStackTrace();
			}
			
			datasetArchiveDir = new File(archiveDir,datasetAlias);
			try
			{
				FileUtils.forceMkdir(datasetArchiveDir);
			}catch(Throwable t)
			{
				t.printStackTrace();
			}
			

			//Insert header
//			File metadataJsonFile = ExternalFileSchema.getSchemaFile(inputFile, logger);
//			if(metadataJsonFile == null || !metadataJsonFile.canRead())
//			{
//				logger.println("Error: metadata Json file {"+metadataJsonFile+"} not found");		
//				return false;
//			}
			
			ExternalFileSchema altSchema = schema;
//			if(DatasetUtilConstants.createNewDateParts)
//				altSchema = ExternalFileSchema.getSchemaWithNewDateParts(schema);
			
			String hdrId = getLastIncompleteFileHdr(partnerConnection, datasetAlias, logger);
			if(hdrId!=null)
			{
				File lastgzbinFile = new File(datasetArchiveDir, hdrId + "." + FilenameUtils.getBaseName(inputFile.getName()) + ".gz");
				if(lastgzbinFile.exists())
				{
					logger.println("Record {"+hdrId+"} is being reused from InsightsExternalData");
					updateHdrJson = true;
				}else
				{
					hdrId = null;
				}
			}

			if(hdrId==null || hdrId.isEmpty())
			{
				hdrId = insertFileHdr(partnerConnection, datasetAlias,datasetFolder, datasetLabel, altSchema.toBytes(), uploadFormat, Operation, notificationLevel,  notificationEmail, logger);
			}
			
			if(hdrId ==null || hdrId.isEmpty())
			{
				logger.println("Error: failed to insert header row into the saleforce SObject");		
				throw new DatasetLoaderException("Error: failed to insert header row into the saleforce SObject");
			}
			
			session.setParam(DatasetUtilConstants.hdrIdParam,hdrId);
			
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

			if(isParsable)
			{	
				long sortStartTime = System.currentTimeMillis();
				File unsortedFile = inputFile;
				inputFile = CsvExternalSort.sortFile(inputFile, inputFileCharset, false, 1, schema, pref);
				long sortEndTime = System.currentTimeMillis();
				if(unsortedFile != inputFile)
				{
					logger.println("\n*******************************************************************************");									
					logger.println(" File {"+inputFile.getName()+"}, sorted in Time {"+nf.format(sortEndTime-sortStartTime) + "} msecs");
					logger.println("*******************************************************************************\n");					
				}
			}
			
			
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

			//Create the Bin file
			//File binFile = new File(csvFile.getParent(), datasetName + ".bin");
			File gzbinFile = inputFile;
			if(!isParsable)
			{
				if(!FilenameUtils.getExtension(inputFile.getName()).equalsIgnoreCase("gz") || !FilenameUtils.getExtension(inputFile.getName()).equalsIgnoreCase("zip"))
				{
					logger.println("\n*******************************************************************************");					
					logger.println("Input file does not have '.csv' extension. Assuming input file is 'ebin' format");
					logger.println("*******************************************************************************\n");					
				}
			}
			
			File lastgzbinFile = new File(datasetArchiveDir, hdrId + "." + FilenameUtils.getBaseName(inputFile.getName()) + ".gz");
			if(!lastgzbinFile.exists())
			{
			if(uploadFormat.equalsIgnoreCase("binary") && isParsable)
			{	
				FileOutputStream fos = null;
				BufferedOutputStream out = null;
				BufferedOutputStream bos = null;
				GzipCompressorOutputStream gzos = null;
				try
				{
				gzbinFile = new File(inputFile.getParent(), hdrId + "." + FilenameUtils.getBaseName(inputFile.getName()) + ".gz");
				GzipParameters gzipParams = new GzipParameters();
				gzipParams.setFilename(FilenameUtils.getBaseName(inputFile.getName())  + ".bin");
				fos = new FileOutputStream(gzbinFile);
				bos = new BufferedOutputStream(fos,DEFAULT_BUFFER_SIZE);
				gzos = new GzipCompressorOutputStream(bos,gzipParams);
				out = new BufferedOutputStream(gzos,DEFAULT_BUFFER_SIZE);
				long totalRowCount = 0;
				long successRowCount = 0;
				long errorRowCount = 0;
				long startTime = System.currentTimeMillis();
				EbinFormatWriter ebinWriter = new EbinFormatWriter(out, schema.getObjects().get(0).getFields().toArray(new FieldType[0]), logger);
				ErrorWriter errorWriter = new ErrorWriter(inputFile,",", pref);
				
				session.setParam(DatasetUtilConstants.errorCsvParam, errorWriter.getErrorFile().getAbsolutePath()); 
				
				CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputFile), false), DatasetUtils.utf8Decoder(codingErrorAction , inputFileCharset )), pref);				
				WriterThread writer = new WriterThread(q, ebinWriter, errorWriter, logger,session);
				Thread th = new Thread(writer,"Writer-Thread");
				th.setDaemon(true);
				th.start();
				
				try
				{
						boolean hasmore = true;
						logger.println("\n*******************************************************************************");					
						logger.println("File: "+inputFile.getName()+", being digested to file: "+gzbinFile.getName());
						logger.println("*******************************************************************************\n");
						if(session!=null)
							session.setStatus("DIGESTING");
						List<String> row = null;
						while (hasmore) 
						{
							if(session.isDone())
							{
								throw new DatasetLoaderException("Operation terminated on user request");
							}
							try
							{
								row = reader.read();
								if(row!=null && !writer.isDone() && !writer.isAborted())
								{
									totalRowCount++;
									if(totalRowCount==1)
										continue;
									if(row.size()!=0 )
									{
										q.put(row.toArray(new String[row.size()]));
									}else
									{
										errorRowCount++;
									}
								}else
								{
									hasmore = false;
								}
							}catch(Exception t)
							{
								errorRowCount++;
//								if(errorRowCount==0)
//								{
//									logger.println();
//								}
								logger.println("Line {"+(totalRowCount)+"} has error {"+t+"}");
								if(t instanceof MalformedInputException)
								{
									q.put(new String[0]);
									int retryCount = 0;
									while(!writer.isDone())
									{
										retryCount++;
										try
										{
											Thread.sleep(1000);
											if(retryCount%10==0)
											{
												q.put(new String[0]);
												logger.println("Waiting for writer to finish");
											}
										}catch(InterruptedException in)
										{
											in.printStackTrace();
										}
									}
									logger.println("\n*******************************************************************************");
									logger.println("The input file is not utf8 encoded. Please save it as UTF8 file first");
									logger.println("*******************************************************************************\n");								
									status = false;
									hasmore = false;
									throw new DatasetLoaderException("The input file is not utf8 encoded");
								}
							}finally
							{
								if(session!=null)
								{
									session.setSourceTotalRowCount(totalRowCount);
									session.setSourceErrorRowCount(errorRowCount);
								}
							}
						}//end while
						int retryCount = 0;
						while(!writer.isDone())
						{
							try
							{
								if(retryCount%10==0)
								{
									q.put(new String[0]);
									logger.println("Waiting for writer to finish");
								}
								Thread.sleep(1000);
							}catch(InterruptedException in)
							{
								in.printStackTrace();
							}
							retryCount++;
						}
					successRowCount = ebinWriter.getSuccessRowCount();
					errorRowCount = writer.getErrorRowCount();
				}finally
				{
					if(reader!=null)
						IOUtils.closeQuietly(reader);
					if(out!=null)
						IOUtils.closeQuietly(out);
					if(gzos!=null)
						IOUtils.closeQuietly(gzos);
					if(bos!=null)
						IOUtils.closeQuietly(bos);
					if(fos!=null)
						IOUtils.closeQuietly(fos);
					out = null;
					gzos = null;
					bos = null;
					fos = null;
					ebinWriter = null;
				}
				long endTime = System.currentTimeMillis();
				digestTime = endTime-startTime;

				//This should never happen
				if(!status)
					return status;
				
				if(writer.isAborted())
				{
					throw new DatasetLoaderException("Max error threshold reached. Aborting processing");
				}
				
				if(successRowCount<1)
				{
					logger.println("\n*******************************************************************************");									
					logger.println("All rows failed. Please check {" + errorWriter.getErrorFile() + "} for error rows");
					logger.println("*******************************************************************************\n");					
					throw new DatasetLoaderException("All rows failed. Please check {" + errorWriter.getErrorFile() + "} for error rows");
				}
				if(errorRowCount>1)
				{
					logger.println("\n*******************************************************************************");									
					logger.println(nf.format(errorRowCount) + " Rows failed. Please check {" + errorWriter.getErrorFile().getName() + "} for error rows");
					logger.println("*******************************************************************************\n");					
				}

				logger.println("\n*******************************************************************************");									
				logger.println("Total Rows: "+nf.format(totalRowCount-1)+", Success Rows: "+nf.format(successRowCount)+", Error Rows: "+nf.format(errorRowCount) +", % Compression: "+(inputFile.length()/gzbinFile.length())*100 +"%"+", Digest Time {"+nf.format(digestTime) + "} msecs");
//				if(gzbinFile.length()>0)
//					logger.println("File: "+inputFile+", Size {"+nf.format(inputFile.length())+"} compressed to file: "+gzbinFile+", Size {"+nf.format(gzbinFile.length())+"} % Compression: "+(inputFile.length()/gzbinFile.length())*100 +"%"+", Digest Time {"+nf.format(digestTime) + "} msecs");
				logger.println("*******************************************************************************\n");					
				} finally {
					if (out != null) {
						try {
							out.close();
							out = null;
						} catch (IOException e) {
						}
					}
					if (gzos != null) {
						try {
							gzos.close();
							gzos = null;
						} catch (IOException e) {
						}
					}
					if (bos != null) {
						try {
							bos.close();
							bos = null;
						} catch (IOException e) {
						}
					}
					if (fos != null) {
						try {
							fos.close();
							fos = null;
						} catch (IOException e) {
						}
					}
				}
			}else if(!FilenameUtils.getExtension(inputFile.getName()).equalsIgnoreCase("zip") && !FilenameUtils.getExtension(inputFile.getName()).equalsIgnoreCase("gz"))
			{
				if(session.isDone())
				{
					throw new DatasetLoaderException("Operation terminated on user request");
				}
				BufferedInputStream fis = null;
				GzipCompressorOutputStream gzOut = null;
				long startTime = System.currentTimeMillis();
				try
				{
					gzbinFile = new File(inputFile.getParent(), FilenameUtils.getBaseName(hdrId + "." + inputFile.getName()) + ".gz");
					logger.println("Input File, will be compressed to gz file {"+gzbinFile+"}");	
					if(session!=null)
						session.setStatus("COMPRESSING");
					GzipParameters gzipParams = new GzipParameters();
					gzipParams.setFilename(inputFile.getName());
					gzOut = new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(gzbinFile),DEFAULT_BUFFER_SIZE),gzipParams);
					fis = new BufferedInputStream(new FileInputStream(inputFile));  
					IOUtils.copy(fis, gzOut);
					long endTime = System.currentTimeMillis();
					if(gzbinFile.length()>0)
						logger.println(" Input File, Size {"+nf.format(inputFile.length())+"} compressed to gz file, Size {"+nf.format(gzbinFile.length())+"} % Compression: "+(inputFile.length()/gzbinFile.length())*100 +"%"+", Compression Time {"+nf.format((endTime-startTime)) + "} msecs");			
				}finally
				{
					  if(gzOut!=null)
					  {
						  try {
							gzOut.close();
							gzOut=null;
						} catch (IOException e) {
						}
					  }
					  if(fis!=null)
					  {
						  try {
							fis.close();
							fis=null;
						} catch (IOException e) {
						}
					  }
				}
			}
			
			if(!gzbinFile.exists() || gzbinFile.length()<1)
			{
				logger.println("Error: File {"+gzbinFile.getAbsolutePath()+"} not found or is zero bytes");
				throw new DatasetLoaderException("Error: File {"+gzbinFile.getAbsolutePath()+"} not found or is zero bytes");
			}else
			{
				if(!inputFile.equals(gzbinFile))
				{
					if(archiveDir.exists())
					{
						try
						{
							FileUtils.moveFile(gzbinFile, lastgzbinFile);
							gzbinFile = lastgzbinFile;
						}catch (Throwable t) {}
					}
				}
			}
			}else
			{
				logger.println("Recovering process from last file {"+lastgzbinFile+"} upload");
				updateHdrJson = false; //The file is already digested, we cannot update the hdr now
				gzbinFile = lastgzbinFile;
			}

			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

			long startTime = System.currentTimeMillis();
			status = uploadEM(gzbinFile, uploadFormat, altSchema.toBytes(), datasetAlias,datasetFolder, datasetLabel,useBulkAPI, partnerConnection, hdrId, datasetArchiveDir, "Overwrite", updateHdrJson, notificationLevel,  notificationEmail, logger);
			long endTime = System.currentTimeMillis();
			uploadTime = endTime-startTime;
			
			if(status)
			{
				String serverStatus = getUploadedFileStatus(partnerConnection, hdrId);
				if(serverStatus!=null)
				{
					session.setParam(DatasetUtilConstants.serverStatusParam,serverStatus.toUpperCase());
					if(serverStatus.equalsIgnoreCase("Failed") || serverStatus.replaceAll(" ", "").equalsIgnoreCase("NotProcessed"))
					{
						status = false;
					}
				}
			}
			
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

		} catch(MalformedInputException mie)
		{
			logger.println("\n*******************************************************************************");
			logger.println("The input file is not valid utf8 encoded. Please save it as UTF8 file first");
			mie.printStackTrace(logger);
			status = false;
			logger.println("*******************************************************************************\n");
			throw new DatasetLoaderException("The input file is not utf8 encoded");
		} catch (Throwable t) {
			logger.println("\n*******************************************************************************");					
			t.printStackTrace(logger);
			status = false;
			logger.println("*******************************************************************************\n");
			throw new DatasetLoaderException(t.getMessage());
		}finally
		{
			if(schemaFile != null && schemaFile.exists() && schemaFile.length()>0)
				session.setParam(DatasetUtilConstants.metadataJsonParam, schemaFile.getAbsolutePath()); 
			
			logger.println("\n*****************************************************************************************************************");					
			if(status)			
				logger.println("Successfully uploaded {"+inputFile+"} to Dataset {"+datasetAlias+"} uploadTime {"+nf.format(uploadTime)+"} msecs" );
			else
				logger.println("Failed to load {"+inputFile+"} to Dataset {"+datasetAlias+"}");
			logger.println("*****************************************************************************************************************\n");
			
			logger.println("\n*******************************************************************************");					
			logger.println("End Timestamp: "+logformat.format(new Date()));
			logger.println("JVM Max memory: "+nf.format(rt.maxMemory()/mb));
			logger.println("JVM Total memory: "+nf.format(rt.totalMemory()/mb));
			logger.println("JVM Free memory: "+nf.format(rt.freeMemory()/mb));			
			logger.println("*******************************************************************************\n");
		}
		return status;
	}

	
	/**
	 * Upload em.
	 *
	 * @param dataFile the data file
	 * @param dataFormat the data format
	 * @param metadataJsonBytes the metadata json bytes
	 * @param datasetAlias the dataset alias
	 * @param datasetFolder the dataset folder
	 * @param datasetLabel the dataset label
	 * @param useBulk the use bulk
	 * @param partnerConnection the partner connection
	 * @param hdrId the hdr id
	 * @param datasetArchiveDir the dataset archive dir
	 * @param Operation the operation
	 * @param updateHdrJson the update hdr json
	 * @param notificationLevel notificationLevel
	 * @param notificationEmail notificationEmail
	 * @param logger the logger
	 * @return true, if successful
	 * @throws DatasetLoaderException the dataset loader exception
	 * @throws InterruptedException the interrupted exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws ConnectionException the connection exception
	 * @throws AsyncApiException the async api exception
	 */
	private static boolean uploadEM(File dataFile, String dataFormat, byte[] metadataJsonBytes, String datasetAlias,String datasetFolder, String datasetLabel, boolean useBulk, PartnerConnection partnerConnection, String hdrId, File datasetArchiveDir, String Operation, boolean updateHdrJson,String notificationLevel, String notificationEmail, PrintStream logger) throws DatasetLoaderException, InterruptedException, IOException, ConnectionException, AsyncApiException 
	{
		BlockingQueue<Map<Integer, File>> q = new LinkedBlockingQueue<Map<Integer, File>>(); 
		LinkedList<Integer> existingFileParts = new LinkedList<Integer>();
		
		String userId;
		try {
			userId = partnerConnection.getUserInfo().getUserId();
		} catch (ConnectionException e) {
			e.printStackTrace(logger);
			throw new DatasetLoaderException("Invalid connection: "+e.getMessage());
		}

		if(datasetAlias==null||datasetAlias.trim().isEmpty())
		{
			throw new DatasetLoaderException("datasetAlias cannot be blank");
		}
		
//		DatasetLoader eu = new DatasetLoader();
		

		logger.println("\n*******************************************************************************");					
		if(datasetFolder != null && datasetFolder.trim().length()!=0)
		{
			logger.println("Uploading dataset {"+datasetAlias+"} to folder {" + datasetFolder + "}");
		}else
		{
			logger.println("Uploading dataset {"+datasetAlias+"} to folder {" + userId +"}");
		}
		logger.println("*******************************************************************************\n");

		ThreadContext tx = ThreadContext.get();
		Session session = tx.getSession();

		if(session.isDone())
		{
			throw new DatasetLoaderException("Operation terminated on user request");
		}

		if(session!=null)
			session.setStatus("UPLOADING");


		if(hdrId==null || hdrId.trim().isEmpty())
		{
			hdrId = insertFileHdr(partnerConnection, datasetAlias,datasetFolder, datasetLabel, metadataJsonBytes, dataFormat, Operation, notificationLevel,  notificationEmail, logger);
		}else
		{
			existingFileParts = getUploadedFileParts(partnerConnection, hdrId);
			if(updateHdrJson && existingFileParts.isEmpty())
				updateFileHdr(partnerConnection, hdrId, datasetAlias, datasetFolder, metadataJsonBytes, dataFormat, "None", Operation, logger);
		}
		
		if(hdrId ==null || hdrId.isEmpty())
		{
			logger.println("Error: failed to insert header row into the saleforce SObject");		
			throw new DatasetLoaderException("Error: failed to insert header row into the saleforce SObject");
		}

		session.setParam(DatasetUtilConstants.hdrIdParam,hdrId);
		
		Map<Integer, File> fileParts = chunkBinary(dataFile, datasetArchiveDir, logger);
		boolean allPartsUploaded = false;
		int retryCount=0; 
		if(fileParts.size()<=MAX_NUM_UPLOAD_THREADS)
		MAX_NUM_UPLOAD_THREADS = 1; 
		while(retryCount<3)
		{
			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}
				q.clear(); //clear the queue otherwise thread will die before it starts because of previous empty messages
				LinkedList<FilePartsUploaderThread> upThreads = new LinkedList<FilePartsUploaderThread>();
				for(int i = 1;i<=MAX_NUM_UPLOAD_THREADS;i++)
				{
					FilePartsUploaderThread writer = new FilePartsUploaderThread(q, partnerConnection, hdrId, logger, session);
					Thread th = new Thread(writer,"FilePartsUploaderThread-"+i);
					th.setDaemon(true);
					th.start();
					upThreads.add(writer);
				}

				if(useBulk)
				{
//						if(eu.insertFilePartsBulk(partnerConnection, hdrId, createBatchZip(fileParts, hdrId, logger), 0, logger))
//							return updateFileHdr(partnerConnection, hdrId, null, null, null, null, "Process", null, logger);
//						else
							return false;					
				}else
				{
					for(int i:fileParts.keySet())
					{
						if(session.isDone())
						{
							throw new DatasetLoaderException("Operation terminated on user request");
						}
						if(!existingFileParts.contains(i))						
						{	
							HashMap<Integer, File> tmp = new HashMap<Integer, File>();
							tmp.put(i,fileParts.get(i));
							q.put(tmp);
						}
					}
					while(!q.isEmpty())
					{
						if(session.isDone())
						{
							throw new DatasetLoaderException("Operation terminated on user request");
						}
						try
						{
							Thread.sleep(1000);
						}catch(InterruptedException in)
						{
							in.printStackTrace();
						}
					}
				}
				
				for(int i = 0;i<MAX_NUM_UPLOAD_THREADS;i++)
				{
					FilePartsUploaderThread uploader = upThreads.get(i);
					while(!uploader.isDone())
					{
						if(session.isDone())
						{
							throw new DatasetLoaderException("Operation terminated on user request");
						}
						q.put(new HashMap<Integer, File>());
						try
						{
							Thread.sleep(1000);
						}catch(InterruptedException in)
						{
							in.printStackTrace();
						}
					}
					logger.println("FilePartsUploaderThread-"+(i+1)+" is done");
				}

				allPartsUploaded = true;
				existingFileParts = getUploadedFileParts(partnerConnection, hdrId);
				for(int i:fileParts.keySet())
				{
					if(!existingFileParts.contains(i))						
					{	
						allPartsUploaded = false;
					}else
					{
						FileUtilsExt.deleteQuietly(fileParts.get(i));
					}
				}
				if(allPartsUploaded)
					break;
				else
					logger.println("Not all file parts uploaded trying again");
				retryCount++;
			}

			if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

				if(allPartsUploaded)
				{
					return updateFileHdr(partnerConnection, hdrId, null, null, null, null, "Process", null, logger);
				}else
				{
					logger.println("Not all file parts were uploaded to InsightsExternalDataPart, remaining files:");
					List<File> remainingFiles = new LinkedList<File>();
					for(int i:fileParts.keySet())
					{
						if(!existingFileParts.contains(i))						
						{	
							logger.println(fileParts.get(i));
							remainingFiles.add(fileParts.get(i));
						}
					}
					throw new DatasetLoaderException("Not all file parts were uploaded to InsightsExternalDataPart, {"+remainingFiles.size()+"} files remaining");
				}
	}

	
	/**
	 * Insert file hdr.
	 *
	 * @param partnerConnection the partner connection
	 * @param datasetAlias the dataset alias
	 * @param datasetContainer the dataset container
	 * @param datasetLabel the dataset label
	 * @param metadataJson the metadata json
	 * @param dataFormat the data format
	 * @param Operation the operation
	 * @param logger the logger
	 * @return the string
	 * @throws DatasetLoaderException the dataset loader exception
	 */
	private static String insertFileHdr(PartnerConnection partnerConnection, String datasetAlias, String datasetContainer, String datasetLabel,  byte[] metadataJson, String dataFormat, String Operation, String notificationLevel, String notificationEmail,PrintStream logger) throws DatasetLoaderException 
	{
		String rowId = null;
		long startTime = System.currentTimeMillis(); 
		try {
			
	    	com.sforce.dataset.Preferences userPref = DatasetUtilConstants.getPreferences(partnerConnection.getUserInfo().getOrganizationId());

			SObject sobj = new SObject();	        
			sobj.setType("InsightsExternalData"); 
	        
	        if(dataFormat == null || dataFormat.equalsIgnoreCase("CSV"))
	        	sobj.setField("Format","CSV");
	        else
	        	sobj.setField("Format","Binary");
    		
	        sobj.setField("EdgemartAlias", datasetAlias);
	        
	        //EdgemartLabel
	        sobj.setField("EdgemartLabel", datasetLabel);
	        
	        if(datasetContainer!=null && !datasetContainer.trim().isEmpty() && !datasetContainer.equals(DatasetUtilConstants.defaultAppName))
	        {
	        	sobj.setField("EdgemartContainer", datasetContainer); //Optional dataset folder name
	        }
	        

	        //sobj.setField("IsIndependentParts",Boolean.FALSE); //Optional Defaults to false
    		
	        //sobj.setField("IsDependentOnLastUpload",Boolean.FALSE); //Optional Defaults to false
    		
    		if(metadataJson != null && metadataJson.length != 0)
    		{
    			sobj.setField("MetadataJson",metadataJson);
    			if(DatasetUtilConstants.debug)
    			{
					logger.println("MetadataJson {"+ new String(metadataJson) + "}");
    			}
    		}
    		
    		if(Operation!=null)
    			sobj.setField("operation",Operation);
    		else
    			sobj.setField("operation","Overwrite");    			
    		
    		sobj.setField("Action","None");
    		
        		//"Always, Failures, Warnings, Never"
    			if(notificationLevel==null || notificationLevel.trim().isEmpty())
    			{
    				if(userPref.notificationLevel==null || userPref.notificationLevel.trim().isEmpty())
        				notificationLevel = "Warnings";
        			else
        				notificationLevel = userPref.notificationLevel;

    			}
    			
    			if(notificationEmail==null || notificationEmail.trim().isEmpty())
    			{
    				if(userPref.notificationEmail==null || userPref.notificationEmail.trim().isEmpty())
    				{
        				GetUserInfoResult userInfo = partnerConnection.getUserInfo();
        				notificationEmail = userInfo.getUserEmail();
    				}
    				else
        				notificationEmail = userPref.notificationEmail;
    			}
    		
        			sobj.setField("NotificationSent",notificationLevel);

            		sobj.setField("NotificationEmail",notificationEmail);
    		
    		
 			//sobj.setField("FileName",fileName);
 		 	//sobj.setField("Description",description);
    		
    		SaveResult[] results = partnerConnection.create(new SObject[] { sobj });    	
    		long endTime = System.currentTimeMillis(); 
    		for(SaveResult sv:results)
    		{ 	
    			if(sv.isSuccess())
    			{
    				rowId = sv.getId();
    				logger.println("Record {"+ sv.getId() + "} Inserted into InsightsExternalData, upload time {"+nf.format(endTime-startTime)+"} msec");
    			}else
    			{
					logger.println("Record {"+ sv.getId() + "} Insert Failed: " + (getErrorMessage(sv.getErrors())));
					throw new DatasetLoaderException("Failed to insert Header into InsightsExternalData Object: " + (getErrorMessage(sv.getErrors())));
    			}
    		}
		} catch (ConnectionException e) {
			e.printStackTrace(logger);
			throw new DatasetLoaderException("Failed to insert Header into InsightsExternalData Object: "+e.getMessage());
		}
		if(rowId==null)
			throw new DatasetLoaderException("Failed to insert Header into InsightsExternalData Object");
			
		return rowId;
	}

	/*
	private boolean insertFileParts(PartnerConnection partnerConnection, String insightsExternalDataId, Map<Integer,File> fileParts, int retryCount) throws Exception 
	{
		LinkedHashMap<Integer,File> failedFileParts = new LinkedHashMap<Integer,File>();
		LinkedList<Integer> existingFileParts = getUploadedFileParts(partnerConnection, insightsExternalDataId);
		for(int i:fileParts.keySet())
		{
			if(existingFileParts.contains(i))
			{
				logger.println("Skipping, File Part {"+ fileParts.get(i) + "}, already Inserted into InsightsExternalDataPart");
				fileParts.get(i).delete();
				continue;
			}
			try {
				long startTime = System.currentTimeMillis(); 
				SObject sobj = new SObject();
		        sobj.setType("InsightsExternalDataPart"); 
	    		sobj.setField("DataFile", FileUtils.readFileToByteArray(fileParts.get(i)));
	    		sobj.setField("InsightsExternalDataId", insightsExternalDataId);
	    		sobj.setField("PartNumber",i); //Part numbers should start at 1	    		
	    		SaveResult[] results = partnerConnection.create(new SObject[] { sobj });				    		
				long endTime = System.currentTimeMillis(); 
	    		for(SaveResult sv:results)
	    		{ 	
	    			if(sv.isSuccess())
	    			{
	    				logger.println("File Part {"+ fileParts.get(i) + "} Inserted into InsightsExternalDataPart: " +sv.getId() + ", upload time {"+nf.format(endTime-startTime)+"} msec");
	    				try
	    				{
	    					fileParts.get(i).delete();
	    				}catch(Throwable t)
	    				{
	    					t.printStackTrace();
	    				}
	    			}else
	    			{
						logger.println("File Part {"+ fileParts.get(i) + "} Insert Failed: " + (getErrorMessage(sv.getErrors())));
						failedFileParts.put(i, fileParts.get(i));
	    			}
	    		}
			} catch (Throwable t) {
				t.printStackTrace();
				logger.println("File Part {"+ fileParts.get(i) + "} Insert Failed: " + t.toString());
				failedFileParts.put(i, fileParts.get(i));
			}
		}
		if(!failedFileParts.isEmpty())
		{
			if(retryCount<3)
			{		
				retryCount++;
				Thread.sleep(1000*retryCount);
//				partnerConnection = DatasetUtils.login(0, username, password, token, endpoint, sessionId);
				return insertFileParts(partnerConnection, insightsExternalDataId, failedFileParts, retryCount);
			}
			else
				return false;
		}
		return true;
	}
	*/

	/**
	 * Insert file parts bulk.
	 *
	 * @param partnerConnection the partner connection
	 * @param insightsExternalDataId the insights external data id
	 * @param fileParts the file parts
	 * @param retryCount the retry count
	 * @param logger the logger
	 * @return true, if successful
	 * @throws ConnectionException the connection exception
	 * @throws AsyncApiException the async api exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unused")
	private boolean insertFilePartsBulk(PartnerConnection partnerConnection, String insightsExternalDataId, Map<Integer,File> fileParts, int retryCount, PrintStream logger) throws ConnectionException, AsyncApiException, IOException 
	{
        BulkConnection bulkConnection = getBulkConnection(partnerConnection.getConfig());
        JobInfo job = createJob("InsightsExternalDataPart", bulkConnection);
        LinkedHashMap<BatchInfo,File> batchInfoList = new LinkedHashMap<BatchInfo,File>();
		for(int i:fileParts.keySet())
		{
		        createBatch(fileParts.get(i), batchInfoList, bulkConnection, job, logger);				
		}
        closeJob(bulkConnection, job.getId());
        awaitCompletion(bulkConnection, job, batchInfoList, logger);
        checkResults(bulkConnection, job, batchInfoList, logger);

		if(!batchInfoList.isEmpty())
		{
			if(retryCount<3)
			{		
				LinkedHashMap<Integer,File> failedFileParts = new LinkedHashMap<Integer,File>();
				for(BatchInfo b:batchInfoList.keySet())
				{
					File temp = batchInfoList.get(b);
					if(temp!=null && temp.exists())
					{
						for(int i:fileParts.keySet())
						{
							File tmp2 = fileParts.get(i);
							if(tmp2!=null && tmp2.exists())
							{
								if(tmp2.equals(temp))
								{
									failedFileParts.put(i, tmp2);
								}
							}
						}
					}
				}
				if(!failedFileParts.isEmpty())
				{
					retryCount++;
					try {
						Thread.sleep(1000*retryCount);
					} catch (InterruptedException e) {
						e.printStackTrace();
						return false;
					}
//					partnerConnection = DatasetUtils.login(0, username, password, token, endpoint, sessionId);
					return insertFilePartsBulk(partnerConnection, insightsExternalDataId, failedFileParts, retryCount, logger);
				}else
				{
					return true;
				}
			}
			else
				return false;
		}
		return true;
	}
	
	
	/**
	 * Update file hdr.
	 *
	 * @param partnerConnection the partner connection
	 * @param rowId the row id
	 * @param datasetAlias the dataset alias
	 * @param datasetContainer the dataset container
	 * @param metadataJson the metadata json
	 * @param dataFormat the data format
	 * @param Action the action
	 * @param Operation the operation
	 * @param logger the logger
	 * @return true, if successful
	 * @throws DatasetLoaderException the dataset loader exception
	 */
	private static boolean updateFileHdr(PartnerConnection partnerConnection, String rowId, String datasetAlias, String datasetContainer, byte[] metadataJson, String dataFormat, String Action, String Operation, PrintStream logger) throws DatasetLoaderException 
	{
		try {
			long startTime = System.currentTimeMillis(); 
			SObject sobj = new SObject();
	        sobj.setType("InsightsExternalData"); 
    		sobj.setId(rowId);
//	        sobj.setField("EdgemartAlias", datasetAlias);
    			        
	        if(dataFormat != null && !dataFormat.isEmpty())
	        {
	        	if(dataFormat.equalsIgnoreCase("CSV"))
	        		sobj.setField("Format","CSV");
	        	else if(dataFormat.equalsIgnoreCase("Binary"))
	        		sobj.setField("Format","Binary");
	        }
    		
	        if(datasetContainer!=null && !datasetContainer.trim().isEmpty())
	        {
	        	sobj.setField("EdgemartContainer", datasetContainer); //Optional dataset folder name
	        }

	        //sobj.setField("IsIndependentParts",Boolean.FALSE); //Optional Defaults to false
    		
	        //sobj.setField("IsDependentOnLastUpload",Boolean.FALSE); //Optional Defaults to false
    		
    		if(metadataJson != null && metadataJson.length != 0)
    			sobj.setField("MetadataJson",metadataJson);
    		
//    		"Overwrite"
    		if(Operation!=null && !Operation.isEmpty())
    		{
    			sobj.setField("operation", Operation);
    		}

    		//Process, None
    		if(Action!=null  && !Action.isEmpty())
    		{
	    		sobj.setField("Action",Action);    		
    		}
    		
    		SaveResult[] results = partnerConnection.update(new SObject[] { sobj });				    		
			long endTime = System.currentTimeMillis(); 
    		for(SaveResult sv:results)
    		{ 	
    			if(sv.isSuccess())
    			{
    				rowId = sv.getId();
    				logger.println("Record {"+ sv.getId() + "} updated in InsightsExternalData"+", upload time {"+nf.format(endTime-startTime)+"} msec");
    			}else
    			{
					logger.println("Record {"+ sv.getId() + "} update Failed: " + (getErrorMessage(sv.getErrors())));
					throw new DatasetLoaderException("Failed to Update Header in InsightsExternalData Object: " + (getErrorMessage(sv.getErrors())));
    			}
    		}
    		return true;
		} catch (ConnectionException e) {
			e.printStackTrace(logger);
			throw new DatasetLoaderException("Failed to update Header in InsightsExternalData Object: "+e.getMessage());
		}
	}
	
	/**
	 * Chunk binary.
	 *
	 * @param inputFile the input file
	 * @param archiveDir the archive dir
	 * @param logger the logger
	 * @return the map
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static Map<Integer,File> chunkBinary(File inputFile, File archiveDir, PrintStream logger) throws IOException 
	{	
		if(inputFile == null)
		{
			throw new IOException("chunkBinary() inputFile parameter is null");
		}
		if(!inputFile.canRead())
		{
			throw new IOException("chunkBinary() cannot read inputFile {"+inputFile+"}");
		}
		if(inputFile.length()==0)
		{
			throw new IOException("chunkBinary() inputFile {"+inputFile+"} is 0 bytes");
		}
//		logger.println("\n*******************************************************************************");					
//		logger.println("Chunking file {"+inputFile+"} into {"+nf.format(DEFAULT_BUFFER_SIZE)+"} size chunks\n");
		long startTime = System.currentTimeMillis();
		InputStream input = null;
		FileOutputStream tmpOut = null;
        LinkedHashMap<Integer,File> fileParts = new LinkedHashMap<Integer,File>();
		try 
		{
			input = new FileInputStream(inputFile);
			byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            Arrays.fill(buffer, (byte)0);
			int n = 0;
			int count = -1;
			int filePartNumber = 0;
			while (EOF != (n = input.read(buffer))) {
				filePartNumber++;
	        	File tmpFile = new File(archiveDir,FilenameUtils.getBaseName(inputFile.getName())+"."+filePartNumber + "." + FilenameUtils.getExtension(inputFile.getName()));
				if(tmpFile != null && tmpFile.exists())
				{
					FileUtilsExt.deleteQuietly(tmpFile);
					if(tmpFile.exists())
					{
						logger.println("Failed to cleanup file {"+tmpFile+"}");
					}
				}
	            tmpOut = new FileOutputStream(tmpFile);			
	            tmpOut.write(buffer, 0, n);
	            Arrays.fill(buffer, (byte)0);
	            tmpOut.close();
	            tmpOut = null;
	            fileParts.put(Integer.valueOf(filePartNumber),tmpFile);
//	            logger.println("Creating File part {"+tmpFile+"}, size {"+nf.format(tmpFile.length())+"}");
				count = ((count == -1) ? n : (count + n));
			}
			if(count == -1)
			{
				throw new IOException("failed to chunkBinary file {"+inputFile+"}");
			}
		}finally {
			if (input != null)
				try {
					input.close();
				} catch (IOException e) {e.printStackTrace();}
			if (tmpOut != null)
				try {
					tmpOut.close();
				} catch (IOException e) {e.printStackTrace();}
		}
		long endTime = System.currentTimeMillis();
		if(fileParts.size()>1)
		{
			logger.println("\n*******************************************************************************");					
			logger.println("\nChunked file into {"+fileParts.size()+"} chunks in {"+nf.format(endTime-startTime)+"} msecs");
			logger.println("*******************************************************************************\n");
		}
		return fileParts;
	} 
	
	/**
	 * Creates the batch zip.
	 *
	 * @param fileParts the file parts
	 * @param insightsExternalDataId the insights external data id
	 * @param logger the logger
	 * @return the map
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unused")
	private static Map<Integer,File> createBatchZip(Map<Integer,File> fileParts,String insightsExternalDataId, PrintStream logger) throws IOException
	{
		LinkedHashMap<Integer,File> zipParts = new LinkedHashMap<Integer,File>();
		for(int i:fileParts.keySet())
		{
				File requestFile = new File(fileParts.get(i).getParent(),"request.txt");
				if(requestFile != null && requestFile.exists())
				{
					FileUtilsExt.deleteQuietly(requestFile);
					if(requestFile.exists())
					{
						logger.println("createBatchZip(): Failed to cleanup file {"+requestFile+"}");
					}
				}				
				String[] row = new String[3];
		        row[0] = insightsExternalDataId;
	    		row[1] = i+"";	    		
	    		row[2] = "#"+fileParts.get(i).getName();
	    		
	    		BufferedWriter fWriter = new BufferedWriter(new FileWriter(requestFile));
	    		boolean first = true;
				for(String val:filePartsHdr)
				{
					if(!first)
						fWriter.write(DatasetLoader.COMMA);
					first = false;
					fWriter.write(val);
				}
				fWriter.write("\n");

	    		
	    	    first = true;
				for(String val:row)
				{
					if(!first)
						fWriter.write(DatasetLoader.COMMA);
					first = false;
					fWriter.write(val);
				}
				fWriter.write("\n");
				fWriter.close();
				fWriter=null;
	    		
	    		File zipFile = new File(fileParts.get(i).getParent(), FilenameUtils.getBaseName(fileParts.get(i).getName()) + ".zip");
				if(zipFile != null && zipFile.exists())
				{
					FileUtilsExt.deleteQuietly(zipFile);
					if(zipFile.exists())
					{
						logger.println("createBatchZip(): Failed to cleanup file {"+zipFile+"}");
					}
				}				
				createZip(zipFile, new File[]{requestFile,fileParts.get(i)}, logger);
				zipParts.put(i,zipFile);
		}
		return zipParts;
	}
	
	/**
	 * Gets the error message.
	 *
	 * @param errors the errors
	 * @return the error message
	 */
	static String getErrorMessage(com.sforce.soap.partner.Error[] errors)
	{
		StringBuffer strBuf = new StringBuffer();
		for(com.sforce.soap.partner.Error err:errors)
		{
		      strBuf.append(" statusCode={");
		      strBuf.append(getCSVFriendlyString(com.sforce.ws.util.Verbose.toString(err.getStatusCode()))+"}");
		      strBuf.append(" message={");
		      strBuf.append(getCSVFriendlyString(com.sforce.ws.util.Verbose.toString(err.getMessage()))+"}");
		      if(err.getFields()!=null && err.getFields().length>0)
		      {
			      strBuf.append(" fields=");
			      strBuf.append(getCSVFriendlyString(com.sforce.ws.util.Verbose.toString(err.getFields())));
		      }
		}
		return strBuf.toString();
	}
	
	
	/**
	 * Gets the CSV friendly string.
	 *
	 * @param content the content
	 * @return the CSV friendly string
	 */
	private static String getCSVFriendlyString(String content)
	{
		if(content!=null && !content.isEmpty())
		{
		content = replaceString(content, "" + COMMA, "");
		content = replaceString(content, "" + CR, "");
		content = replaceString(content, "" + LF, "");
		content = replaceString(content, "" + QUOTE, "");
		}
		return content;
	}
	
	
	/**
	 * Replace string.
	 *
	 * @param original the original
	 * @param pattern the pattern
	 * @param replace the replace
	 * @return the string
	 */
	private static String replaceString(String original, String pattern, String replace) 
	{
		if(original != null && !original.isEmpty() && pattern != null && !pattern.isEmpty() && replace !=null)
		{
			final int len = pattern.length();
			int found = original.indexOf(pattern);

			if (found > -1) {
				StringBuffer sb = new StringBuffer();
				int start = 0;

				while (found != -1) {
					sb.append(original.substring(start, found));
					sb.append(replace);
					start = found + len;
					found = original.indexOf(pattern, start);
				}

				sb.append(original.substring(start));

				return sb.toString();
			} else {
				return original;
			}
		}else
			return original;
	}
	
	
	   /**
   	 * Check results.
   	 *
   	 * @param connection the connection
   	 * @param job the job
   	 * @param batchInfoList the batch info list
   	 * @param logger the logger
   	 * @throws AsyncApiException the async api exception
   	 * @throws IOException Signals that an I/O exception has occurred.
   	 */
    private static void checkResults(BulkConnection connection, JobInfo job,
    		LinkedHashMap<BatchInfo,File> batchInfoList, PrintStream logger)
            throws AsyncApiException, IOException {
    	@SuppressWarnings("unchecked")
    	LinkedHashMap<BatchInfo,File> tmp = (LinkedHashMap<BatchInfo, File>) batchInfoList.clone();
        for (BatchInfo b : tmp.keySet()) {
            CSVReader rdr =
              new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new LinkedHashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));
                if (success && created) {
                    String id = resultInfo.get("id");
//                    logger.println("Created row with id " + id);
    				logger.println("File Part {"+ batchInfoList.get(b) + "} Inserted into InsightsExternalDataPart: " +id);
    				File f = batchInfoList.remove(b);
    				try
    				{
    					if(f != null && f.exists())
    					{
    						FileUtilsExt.deleteQuietly(f);
	    					if(f.exists())
	    					{
	    						logger.println("Failed to cleanup file {"+f+"}");
	    					}
    					}
    				}catch(Throwable t)
    				{
						logger.println("Failed to cleanup file {"+f+"}");
    					t.printStackTrace();
    				}
                } else if (!success) {
                    String error = resultInfo.get("Error");
//                    logger.println("Failed with error: " + error);
					logger.println("File Part {"+ batchInfoList.get(b) + "} Insert Failed: " + error);
                }
            }
        }
    }



    /**
     * Close job.
     *
     * @param connection the connection
     * @param jobId the job id
     * @throws AsyncApiException the async api exception
     */
    private static void closeJob(BulkConnection connection, String jobId)
          throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }



    /**
     * Await completion.
     *
     * @param connection the connection
     * @param job the job
     * @param batchInfoList the batch info list
     * @param logger the logger
     * @throws AsyncApiException the async api exception
     */
    private static void awaitCompletion(BulkConnection connection, JobInfo job,
    		LinkedHashMap<BatchInfo,File> batchInfoList, PrintStream logger)
            throws AsyncApiException {
        long sleepTime = 0L;
        Set<String> incomplete = new LinkedHashSet<String>();
        for (BatchInfo bi : batchInfoList.keySet()) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            logger.println("Awaiting Async Batch results. Incomplete Batch Size {" + incomplete.size() + "}");
            sleepTime = 10000L;
            BatchInfo[] statusList =
              connection.getBatchInfoList(job.getId()).getBatchInfo();
            for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed
                  || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
//                        logger.println("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }



    /**
     * Creates the job.
     *
     * @param sobjectType the sobject type
     * @param connection the connection
     * @return the job info
     * @throws AsyncApiException the async api exception
     */
    private static JobInfo createJob(String sobjectType, BulkConnection connection)
          throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(OperationEnum.insert);
        job.setContentType(ContentType.ZIP_CSV);
        job = connection.createJob(job);
//        logger.println(job);
        return job;
    }

    

    /**
     * Gets the bulk connection.
     *
     * @param partnerConfig the partner config
     * @return the bulk connection
     * @throws ConnectionException the connection exception
     * @throws AsyncApiException the async api exception
     */
    private static BulkConnection getBulkConnection(ConnectorConfig partnerConfig)
          throws ConnectionException, AsyncApiException {
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        // The endpoint for the Bulk API service is the same as for the normal
        // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String apiVersion = "31.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
            + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        // This should only be false when doing debugging.
        config.setCompression(true);
        // Set this to true to see HTTP requests and responses on stdout
        config.setTraceMessage(false);
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }

    
    /**
     * Creates the batch.
     *
     * @param zipFile the zip file
     * @param batchInfos the batch infos
     * @param connection the connection
     * @param jobInfo the job info
     * @param logger the logger
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws AsyncApiException the async api exception
     */
    private void createBatch(File zipFile,
    		LinkedHashMap<BatchInfo,File> batchInfos, BulkConnection connection, JobInfo jobInfo, PrintStream logger)
    	              throws IOException, AsyncApiException {
    	        FileInputStream zipFileStream = new FileInputStream(zipFile);
    	        try {
    	    		logger.println("creating bulk api batch for file {"+zipFile+"}");    	        	
    	        	BatchInfo batchInfo = connection.createBatchFromZipStream(jobInfo, zipFileStream);
//    	            logger.println(batchInfo);
    	            batchInfos.put(batchInfo, zipFile);
    	        } finally {
    	            zipFileStream.close();
    	        }
    	    }


	/**
	 * Creates the zip.
	 *
	 * @param zipfile the zipfile
	 * @param files the files
	 * @param logger the logger
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void createZip(File zipfile,File[] files, PrintStream logger) throws IOException
	{
		if(zipfile == null)
		{
			throw new IOException("createZip(): called with null zipfile parameter");
		}
		if(files == null || files.length==0)
		{
			throw new IOException("createZip(): called with null files parameter");
		}
		logger.println("creating batch request zip file {"+zipfile+"}");
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(zipfile));
		ZipOutputStream zip = new ZipOutputStream(bos);
		for(File file:files)
		{
			if(file == null)
			{
				throw new IOException("createZip(): called with null files parameter");
			}
			if(!file.exists())
			{
				throw new IOException("createZip(): called with file {"+file+"} that does not exist");
			}
			ZipEntry zipEntry = new ZipEntry(file.getName());
			zip.putNextEntry(zipEntry);
			FileInputStream origin = new FileInputStream(file);
			IOUtils.copy(origin, zip);
			zip.closeEntry();
			origin.close();
			origin = null;
		}
		zip.close();
		bos.close();
		zip = null;
		bos = null;
		for(File file:files)
		{
			try
			{
				if(file != null && file.exists())
				{
					FileUtilsExt.deleteQuietly(file);
					if(file.exists())
					{
						logger.println("createZip(): Failed to cleanup file {"+file+"}");
					}
				}
			}catch(Throwable t)
			{
				logger.println("createZip(): Failed to cleanup file {"+file+"}");
				t.printStackTrace();
			}
		}

	}


	/*
	public static boolean checkAPIAccess(String username2, String password2,
			String token2, String endpoint2, String sessionId2) {
		try {
			PartnerConnection partnerConnection = DatasetUtils.login(0, username2, password2, token2, endpoint2, sessionId2);
			Map<String,String> objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+"InsightsExternalData"+"\\b"), false);
			if(objectList==null || objectList.size()==0)
			{
				logger.println("\n");
				logger.println("Error: Object {"+"InsightsExternalData"+"} not found");
				return false;
			}
			objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+"InsightsExternalDataPart"+"\\b"), false);
			if(objectList==null || objectList.size()==0)
			{
				logger.println("\n");
				logger.println("Error: Object {"+"InsightsExternalDataPart"+"} not found");
				return false;
			}
			return true;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return false;
	}
	*/

	/**
	 * Check api access.
	 *
	 * @param partnerConnection the partner connection
	 * @param logger the logger
	 * @return true, if successful
	 */
	private static boolean checkAPIAccess(PartnerConnection partnerConnection, PrintStream logger) {
		try {
			Map<String,String> objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+"InsightsExternalData"+"\\b"), false);
			if(objectList==null || objectList.size()==0)
			{
				logger.println("\n");
				logger.println("Error: Object {"+"InsightsExternalData"+"} not found");
				return false;
			}
			objectList = SfdcUtils.getObjectList(partnerConnection, Pattern.compile("\\b"+"InsightsExternalDataPart"+"\\b"), false);
			if(objectList==null || objectList.size()==0)
			{
				logger.println("\n");
				logger.println("Error: Object {"+"InsightsExternalDataPart"+"} not found");
				return false;
			}
			return true;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return false;
	}
	

	/**
	 * Gets the last incomplete file hdr.
	 *
	 * @param partnerConnection the partner connection
	 * @param datasetAlias the dataset alias
	 * @param logger the logger
	 * @return the last incomplete file hdr
	 * @throws Exception the exception
	 */
	private static String getLastIncompleteFileHdr(PartnerConnection partnerConnection, String datasetAlias, PrintStream logger) throws Exception 
	{
		String rowId = null;
		String soqlQuery = String.format("SELECT id,Status,Action FROM InsightsExternalData WHERE EdgemartAlias = '%s' ORDER BY CreatedDate DESC LIMIT 1",datasetAlias);
		partnerConnection.setQueryOptions(2000);
		QueryResult qr = partnerConnection.query(soqlQuery);
		int rowsSoFar = 0;
		boolean done = false;
		if (qr.getSize() > 0) 
		{
			while (!done) 
			{
				SObject[] records = qr.getRecords();
				for (int i = 0; i < records.length; ++i) 
				{
					if(rowsSoFar==0) //only get the first one
					{
						String fieldName = "id";
						Object value = SfdcUtils.getFieldValueFromQueryResult(fieldName,records[i]);
						fieldName = "Status";
						Object Status = SfdcUtils.getFieldValueFromQueryResult(fieldName,records[i]);
						fieldName = "Action";
						Object Action = SfdcUtils.getFieldValueFromQueryResult(fieldName,records[i]);
						if (value != null && Status != null && Status.toString().equalsIgnoreCase("new") && Action != null && Action.toString().equalsIgnoreCase("none")) {
								rowId = value.toString();
						}
					}
					rowsSoFar++;
				}
				if (qr.isDone()) {
					done = true;
				} else {
					qr = partnerConnection.queryMore(qr.getQueryLocator());
				}
			}// End While
		}
		if(rowsSoFar>1)
		{
			logger.println("getLastIncompleteFileHdr() returned more than one row");
		}
		return rowId; 
	}
	
	/**
	 * Gets the uploaded file parts.
	 *
	 * @param partnerConnection the partner connection
	 * @param hdrId the hdr id
	 * @return the uploaded file parts
	 * @throws ConnectionException the connection exception
	 */
	private static LinkedList<Integer> getUploadedFileParts(PartnerConnection partnerConnection, String hdrId) throws ConnectionException 
	{
		LinkedList<Integer> existingPartList = new LinkedList<Integer>();
		String soqlQuery = String.format("SELECT id,PartNumber FROM InsightsExternalDataPart WHERE InsightsExternalDataId = '%s' ORDER BY PartNumber ASC",hdrId);
		partnerConnection.setQueryOptions(2000);
		QueryResult qr = partnerConnection.query(soqlQuery);
		boolean done = false;
		if (qr.getSize() > 0) 
		{
			while (!done) 
			{
				SObject[] records = qr.getRecords();
				for (int i = 0; i < records.length; ++i) 
				{
						Object value = SfdcUtils.getFieldValueFromQueryResult("PartNumber",records[i]);
						if (value != null) {
							if(value instanceof Integer)
								existingPartList.add((Integer)value);
							else
								existingPartList.add(new Integer(value.toString()));
						}
				}
				if (qr.isDone()) {
					done = true;
				} else {
					qr = partnerConnection.queryMore(qr.getQueryLocator());
				}
			}// End While
		}
		return existingPartList; 
	}	

	
	/**
	 * Gets the uploaded file status.
	 *
	 * @param partnerConnection the partner connection
	 * @param hdrId the hdr id
	 * @return the uploaded file status
	 * @throws ConnectionException the connection exception
	 */
	public static String getUploadedFileStatus(PartnerConnection partnerConnection, String hdrId) throws ConnectionException 
	{
		String status = null;
		String soqlQuery = String.format("SELECT Status FROM InsightsExternalData WHERE Id = '%s'",hdrId);
		partnerConnection.setQueryOptions(2000);
		QueryResult qr = partnerConnection.query(soqlQuery);
		boolean done = false;
		if (qr.getSize() > 0) 
		{
			while (!done) 
			{
				SObject[] records = qr.getRecords();
				for (int i = 0; i < records.length; ++i) 
				{
						Object value = SfdcUtils.getFieldValueFromQueryResult("Status",records[i]);
						if (value != null) {
							status = value.toString();
						}
				}
				if (qr.isDone()) {
					done = true;
				} else {
					qr = partnerConnection.queryMore(qr.getQueryLocator());
				}
			}// End While
		}
		return status; 
	}
	
	
	/**
	 * Gets the last uploaded json.
	 *
	 * @param partnerConnection the partner connection
	 * @param datasetAlias the dataset alias
	 * @param logger the logger
	 * @return the last uploaded json
	 * @throws Exception the exception
	 */
	public static ExternalFileSchema getLastUploadedJson(PartnerConnection partnerConnection, String datasetAlias, PrintStream logger) throws Exception 
	{
		String json = null;
		ExternalFileSchema schema = null;
		String soqlQuery = String.format("SELECT Id,Status,MetadataJson FROM InsightsExternalData WHERE Status = 'Completed' AND EdgemartAlias = '%s' ORDER BY LastModifiedDate DESC LIMIT 1",datasetAlias);
		partnerConnection.setQueryOptions(2000);
		QueryResult qr = partnerConnection.query(soqlQuery);
		int rowsSoFar = 0;
		boolean done = false;
		if (qr.getSize() > 0) 
		{
			while (!done) 
			{
				SObject[] records = qr.getRecords();
				for (int i = 0; i < records.length; ++i) 
				{
					if(rowsSoFar==0) //only get the first one
					{
						String fieldName = "Id";
						Object value = SfdcUtils.getFieldValueFromQueryResult(fieldName,records[i]);
						fieldName = "Status";
						Object Status = SfdcUtils.getFieldValueFromQueryResult(fieldName,records[i]);
						fieldName = "MetadataJson";
						if (value != null && Status != null && Status.toString().equalsIgnoreCase("Completed")) 
						{
								Object temp = SfdcUtils.getFieldValueFromQueryResult(fieldName,records[i]);
								if(temp!=null)
								{
									if(temp instanceof byte[])
									{
										json = IOUtils.toString((byte[])temp, "UTF-8");										
									}else if(temp instanceof InputStream)
									{
										json = IOUtils.toString((InputStream)temp, "UTF-8");
									}else
									{
										String str = temp.toString();
										if(Base64.isBase64(str))
										{
											json = IOUtils.toString(Base64.decode(str.getBytes()),"UTF-8");
										}else
										{
											json = str;
										}
									}
								}
						}
					}
					rowsSoFar++;
				}
				if (qr.isDone()) {
					done = true;
				} else {
					qr = partnerConnection.queryMore(qr.getQueryLocator());
				}
			}// End While
		}
		if(rowsSoFar>1)
		{
			logger.println("getLastIncompleteFileHdr() returned more than one row");
		}

		if(json != null)
		{
			schema = ExternalFileSchema.load(IOUtils.toInputStream(json), DatasetUtils.utf8Charset, logger);
		}
		return schema; 
	}


	 
}

