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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
 
public class FileListenerThread implements Runnable {
	
//private static final String success = "Success";
//private static final String error = "Error";
//private static final String sessionLog = "Logs";


  private volatile boolean isDone = false;

  private final  FileListener fileListener;
  private final PartnerConnection partnerConnection;
//  private final File errorDir;
//  private final File successDir;
//  private final File logsDir;
//  private final Session session;
  
  FileListenerThread(FileListener fileListener, PartnerConnection partnerConnection) throws IOException, ConnectionException 
  { 
	  if(fileListener==null)
	  {
		  throw new IllegalArgumentException("Constructor input cannot be null");
	  }
	  this.fileListener = fileListener;
	  this.partnerConnection = partnerConnection;
//	  errorDir = new File(this.fileListener.fileDir,success); 
//	  successDir = new File(this.fileListener.fileDir,error); 
//	  logsDir = new File(this.fileListener.fileDir, sessionLog);
//	  FileUtils.forceMkdir(errorDir);
//	  FileUtils.forceMkdir(successDir);
//	  FileUtils.forceMkdir(logsDir);
  }
 
public void run() {
 		System.out.println("Starting FileListener for Dataset {"+fileListener.getDataset()+"} ");
 		
    try {

    	while (!isDone) {
			try
			{
				long cutOff = System.currentTimeMillis() - (fileListener.getFileAge());
				IOFileFilter ageFilter = FileFilterUtils.ageFileFilter(cutOff);
				IOFileFilter nameFilter = FileFilterUtils.nameFileFilter(fileListener.getInputFilePattern(), IOCase.INSENSITIVE);
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
						t.printStackTrace();
					}
					if(isDone)
						break;
					else
						continue;
				}
				
				for(File file:files)
				{
					PrintStream logger = null;
					Session session = null;
					try
					{
						String orgId = null;
						orgId = partnerConnection.getUserInfo().getOrganizationId();
						session = Session.getCurrentSession(orgId, fileListener.getDataset());
//						session = new Session(orgId,fileListener.getDataset());
//				        ThreadContext threadContext = ThreadContext.get();
//				        threadContext.setSession(session);
				        session.start();
//						long timeStamp = System.currentTimeMillis();
//						File logFile = new File(logsDir,FilenameUtils.getBaseName(file.getName())+timeStamp+".log");
						File logFile = session.getSessionLog();
						logger = new PrintStream(new FileOutputStream(logFile), true, "UTF-8");
						boolean status = DatasetLoader.uploadDataset(file.toString(), fileListener.getUploadFormat(), fileListener.cea, fileListener.charset, fileListener.getDataset(), fileListener.getApp(), fileListener.getDatasetLabel(), fileListener.getOperation(), fileListener.isUseBulkAPI(), partnerConnection, logger);
						if(status)
							session.end();
						else
							session.fail("Check sessionLog for details");
						moveInputFile(file, status, session);
					}catch(Throwable t)
					{
						if(logger!=null)
							t.printStackTrace(logger);
						else
							t.printStackTrace();

						if(session!=null)
							session.fail("Check sessionLog for details");
					}finally
					{
						if(logger!=null)
							logger.close();
						logger = null;
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
	System.out.println("Starting FileListener for Dataset {"+fileListener.getDataset()+"} ");
    isDone = true;
  }

public boolean isDone() {
	return isDone;
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
	
	
	public static void moveInputFile(File inputFile, boolean isSuccess, Session session) 
	{
		if(inputFile == null)
			return;

		if(!inputFile.exists())
			return;

		if(inputFile.isDirectory())
			return;

//		File parent = inputFile.getAbsoluteFile().getParentFile();
			
//		File directory = new File(parent,success);
//		if(!isSuccess)
//			directory = new File(parent,error);
			
		File directory = DatasetUtilConstants.getSuccessDir(session.getOrgId());
		if(!isSuccess)
			directory = DatasetUtilConstants.getErrorDir(session.getOrgId());
			
		File doneFile = new File(directory, FilenameUtils.getBaseName(inputFile.getName())+"_"+session.getId()+"."+FilenameUtils.getExtension(inputFile.getName()));
		try {
			FileUtils.moveFile(inputFile, doneFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File sortedtFile = new File(inputFile.getParent(), FilenameUtils.getBaseName(inputFile.getName())+ "_sorted." + FilenameUtils.getExtension(inputFile.getName()));
		if(sortedtFile.exists())
		{
			File sortedDoneFile = new File(directory,FilenameUtils.getBaseName(sortedtFile.getName())+"_"+session.getId()+"."+FilenameUtils.getExtension(sortedtFile.getName()));
			try {
				FileUtils.moveFile(sortedtFile, sortedDoneFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}



}