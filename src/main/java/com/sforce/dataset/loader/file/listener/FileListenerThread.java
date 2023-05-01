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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.dataset.loader.ErrorWriter;
import com.sforce.dataset.loader.file.schema.ExternalFileSchema;
import com.sforce.dataset.util.FileUtilsExt;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
 
public class FileListenerThread implements Runnable {
	
private static final String work = "work";
private static final String success = "success";
private static final String error = "error";
//private static final String sessionLog = "logs";


  private AtomicBoolean isDone = new AtomicBoolean(false);

  private final  FileListener fileListener;
  private final PartnerConnection partnerConnection;
  private final File workDir;
  private final File errorDir;
  private final File successDir;
//  private final File logsDir;
  private final File jsonFile;
  private Session session = null;
  
  public FileListenerThread(FileListener fileListener, PartnerConnection partnerConnection) throws IOException, ConnectionException 
  { 
	  if(fileListener==null || partnerConnection == null)
	  {
		  throw new IllegalArgumentException("Constructor input cannot be null");
	  }
	  this.fileListener = fileListener;
	  this.partnerConnection = partnerConnection;
	  workDir = new File(this.fileListener.fileDir.getAbsoluteFile().getParentFile(),work); 
	  errorDir = new File(this.fileListener.fileDir.getAbsoluteFile().getParentFile(),error); 
	  successDir = new File(this.fileListener.fileDir.getAbsoluteFile().getParentFile(),success); 
//	  logsDir = new File(this.fileListener.fileDir.getAbsoluteFile().getParentFile(), sessionLog);
	  FileUtils.forceMkdir(workDir);
	  FileUtils.forceMkdir(errorDir);
	  FileUtils.forceMkdir(successDir);
//	  FileUtils.forceMkdir(logsDir);
	  jsonFile = new File(this.fileListener.fileDir.getAbsoluteFile(),this.fileListener.getDatasetAlias() + ExternalFileSchema.SCHEMA_FILE_SUFFIX);

  }
 
public void run() {
 		System.out.println("Starting FileListener for Dataset {"+fileListener.getDatasetAlias()+"} ");
 		
    try {

    	while (!isDone()) {
			try
			{
				long cutOff = System.currentTimeMillis() - (fileListener.getFileAge());
				IOFileFilter ageFilter = FileFilterUtils.ageFileFilter(cutOff);
//				IOFileFilter nameFilter = FileFilterUtils.nameFileFilter(fileListener.getInputFilePattern(), IOCase.INSENSITIVE);
				IOFileFilter nameFilter = new WildcardFileFilter(fileListener.getInputFilePattern(),IOCase.INSENSITIVE);
//				IOFileFilter suffixFileFilter1 = FileFilterUtils.suffixFileFilter(".zip", IOCase.INSENSITIVE);
//				IOFileFilter suffixFileFilter2 = FileFilterUtils.suffixFileFilter(".csv", IOCase.INSENSITIVE);
//				IOFileFilter orFilter = FileFilterUtils.and(suffixFileFilter1, suffixFileFilter2);				
				IOFileFilter andFilter = FileFilterUtils.and(nameFilter, ageFilter);
				
				File[] files = getFiles(fileListener.fileDir, andFilter);
				if (files == null) 
				{
					try
					{
						Thread.sleep(fileListener.getPollingInterval());
					}catch(Throwable t)
					{
//						t.printStackTrace();
					}
					if(isDone())
						break;
					else
						continue;
				}
				
				for(File file:files)
				{
					PrintStream logger = null;
					boolean status = false;
					File workFile = null;
					try
					{
						String orgId = null;
						orgId = partnerConnection.getUserInfo().getOrganizationId();
						session = Session.getCurrentSession(orgId, fileListener.getDatasetAlias(), true);
//						session = new Session(orgId,fileListener.getDataset());
//				        ThreadContext threadContext = ThreadContext.get();
//				        threadContext.setSession(session);
				        workFile = setup(file, session);
				        if(workFile==null || !workFile.exists())
				        {
				        	continue;
				        }
				        session.start();
//						long timeStamp = System.currentTimeMillis();
//						File logFile = new File(logsDir,FilenameUtils.getBaseName(file.getName())+timeStamp+".log");
						File logFile = session.getSessionLog();
						logger = new PrintStream(new FileOutputStream(logFile), true, "UTF-8");
						status = DatasetLoader.uploadDataset(workFile.toString(),null, fileListener.getUploadFormat(), fileListener.cea, fileListener.charset, fileListener.getDatasetAlias(),
								fileListener.getDatasetApp(), fileListener.getDatasetLabel(), fileListener.getOperation(), fileListener.isUseBulkAPI(),fileListener.getChunkSizeMulti(), partnerConnection,
								fileListener.getNotificationLevel(), fileListener.getNotificationEmail(),
								fileListener.getMode(), logger);
						if(workFile!=null && workFile.exists())
						{
							cleanup(workFile, status, session);
							workFile = null;
						}
						if(status)
							session.end();
						else
							session.fail("Check sessionLog for details");
					}catch(Throwable t)
					{
						status = false;
						if(logger!=null)
							t.printStackTrace(logger);
						else
							t.printStackTrace();

						if(workFile!=null && workFile.exists())
						{
							cleanup(workFile, status, session);
						}
						if(session!=null)
							session.fail("Check sessionLog for details");
					}finally
					{
						if(logger!=null)
							logger.close();
						logger = null;
						session = null;
					}
				}
				
			}catch(Throwable t)
			{
				t.printStackTrace();
			}
       }
    }catch (Throwable t) {
       System.out.println (Thread.currentThread().getName() + " " + t.getMessage());
    }
	System.out.println("Stopping FileListener for Dataset {"+fileListener.getDatasetAlias()+"} ");
    isDone.set(true);
    session = null;
  }

public boolean isDone() {
	return isDone.get();
}
  
	public static File[] getFiles(File directory, IOFileFilter fileFilter) {
//		 File[] files = directory.listFiles(fileFilter);
//		Collection<File> list = FileUtils.listFiles(directory, fileFilter,TrueFileFilter.INSTANCE);
		if(directory==null)
			directory = new File("").getAbsoluteFile();
		FileUtils.listFiles(directory, fileFilter, null);
		Collection<File> list = FileUtils.listFiles(directory, fileFilter, null);

		File[] files = list.toArray(new File[0]);

		if (files != null && files.length > 0) {
			Arrays.sort(files, new Comparator<File>() {
				public int compare(File a, File b) {
					long diff = (a.lastModified() - b.lastModified());
					if(diff>0L)
						return 1;
					else if(diff<0L)
						return -1;
					else
						return 0;
				}
			});

			//for (File file : files) {
			//	Date lastMod = new Date(file.lastModified());
			//	System.out.println("Found File {" + file.getName() + "}, lastModified {"+ lastMod + "}");
			//}
			return files;
		} else {
			return null;
		}
	}
	
	public File setup(File inputFile, Session session) throws IOException 
	{
		if(inputFile == null || !inputFile.exists())
		{
			throw new IllegalArgumentException("File {"+inputFile+"} does not exist");
		}
		
		if(inputFile.isDirectory())
		{
			throw new IllegalArgumentException("File {"+inputFile+"} is a directory");
		}
			
		File workFile = new File(workDir, FilenameUtils.getBaseName(inputFile.getName())+"_"+session.getId()+"."+FilenameUtils.getExtension(inputFile.getName()));
		FileUtils.moveFile(inputFile, workFile);
		if(!workFile.exists())
		{
			return null;
		}

		File jsonInputFile =  com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema.getSchemaFile(inputFile, System.out);
		File jsonWorkFile =  com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema.getSchemaFile(workFile, System.out);
		if(jsonInputFile.exists())
		{
			FileUtils.copyFile(jsonInputFile, jsonWorkFile);
		}else
		{		
			if(jsonFile.exists())
			{
				FileUtils.copyFile(jsonFile, jsonWorkFile);
			}
		}
		return workFile;
	}
	
	public void cleanup(File inputFile, boolean isSuccess, Session session) 
	{
		if(inputFile == null)
			return;

		if(!inputFile.exists())
			return;

		if(inputFile.isDirectory())
			return;

		File directory = this.successDir;
		if(!isSuccess)
			directory = this.errorDir;
			
		File doneFile = new File(directory, inputFile.getName());
		File sortedtFile = new File(inputFile.getParent(), FilenameUtils.getBaseName(inputFile.getName())+ "_sorted." + FilenameUtils.getExtension(inputFile.getName()));
		File jsonWorkFile =  com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema.getSchemaFile(inputFile, System.out);
		File errorFile = new File(inputFile.getParent(), FilenameUtils.getBaseName(inputFile.getName())+ ErrorWriter.errorFileSuffix + FilenameUtils.getExtension((inputFile.getName())));

		try {
			FileUtils.moveFile(inputFile, doneFile);
		} catch (IOException e) {
			e.printStackTrace();
			FileUtilsExt.deleteQuietly(inputFile);
		}
		
		if(sortedtFile.exists())
		{
			File sortedDoneFile = new File(directory,sortedtFile.getName());
			try {
				FileUtils.moveFile(sortedtFile, sortedDoneFile);
			} catch (IOException e) {
				e.printStackTrace();
				FileUtilsExt.deleteQuietly(sortedtFile);
			}
		}
		
		if(errorFile.exists())
		{
			File errorDoneFile = new File(directory,errorFile.getName());
			try {
				FileUtils.moveFile(errorFile, errorDoneFile);
			} catch (IOException e) {
				e.printStackTrace();
				FileUtilsExt.deleteQuietly(errorFile);
			}
			if(errorDoneFile.exists())
			{
				session.setParam(DatasetUtilConstants.errorCsvParam, errorDoneFile.getAbsolutePath());
			}
		}
		
		
		if(jsonWorkFile.exists())
		{
			File jsonDoneFile = new File(directory,jsonWorkFile.getName());
				try {
					FileUtils.moveFile(jsonWorkFile, jsonDoneFile);
				} catch (IOException e) {
					e.printStackTrace();
				}
			if(jsonDoneFile.exists())
			{
				session.setParam(DatasetUtilConstants.metadataJsonParam, jsonDoneFile.getAbsolutePath());
			}
		}
	}

	public void stop() {
		isDone.set(true);
		if(session!=null)
			session.terminate(null);
	}



}