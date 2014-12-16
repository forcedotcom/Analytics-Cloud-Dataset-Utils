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

import com.sforce.dataset.loader.DatasetLoader;
import com.sforce.soap.partner.PartnerConnection;
 
public class FileListenerThread implements Runnable {
	

  private volatile boolean isDone = false;

  private final  FileListener fileListener;
  private final PartnerConnection partnerConnection;
  private static final File errorDir = new File("error");
  private static final File successDir = new File("success");
  private static final File logsDir = new File("logs");
  
  FileListenerThread(FileListener fileListener, PartnerConnection partnerConnection) throws IOException 
  { 
	  if(fileListener==null)
	  {
		  throw new IllegalArgumentException("Constructor input cannot be null");
	  }
	  this.fileListener = fileListener;
	  this.partnerConnection = partnerConnection;
//	   errorDir = new File("Error"); 
//	   successDir = new File("Success"); 
	  FileUtils.forceMkdir(errorDir);
	  FileUtils.forceMkdir(successDir);
	  FileUtils.forceMkdir(logsDir);
  }
 
public void run() {
 		System.out.println("Starting FileListener for Dataset {"+fileListener.dataset+"} ");
 		
    try {
       while (!isDone) {
			try
			{
				long cutOff = System.currentTimeMillis() - (fileListener.fileAge);
				IOFileFilter ageFilter = FileFilterUtils.ageFileFilter(cutOff);
				IOFileFilter nameFilter = FileFilterUtils.nameFileFilter(fileListener.inputFilePattern, IOCase.INSENSITIVE);
//				IOFileFilter suffixFileFilter1 = FileFilterUtils.suffixFileFilter(".zip", IOCase.INSENSITIVE);
//				IOFileFilter suffixFileFilter2 = FileFilterUtils.suffixFileFilter(".csv", IOCase.INSENSITIVE);
//				IOFileFilter orFilter = FileFilterUtils.and(suffixFileFilter1, suffixFileFilter2);				
				IOFileFilter andFilter = FileFilterUtils.and(nameFilter, ageFilter);
				
				File[] files = getFiles(fileListener.fileDir, andFilter);
				if (files == null) 
				{
					try
					{
						Thread.sleep(fileListener.pollingInterval);
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
					try
					{
						long timeStamp = System.currentTimeMillis();
						File logFile = new File(logsDir,FilenameUtils.getBaseName(file.getName())+timeStamp+".log");
						logger = new PrintStream(new FileOutputStream(logFile), true, "UTF-8");
						boolean status = DatasetLoader.uploadDataset(file.toString(), fileListener.uploadFormat, fileListener.cea, fileListener.charset, fileListener.dataset, fileListener.app, fileListener.datasetLabel, fileListener.operation, fileListener.useBulkAPI, partnerConnection, logger);
						moveInputFile(file, timeStamp, status);
					}catch(Throwable t)
					{
						if(logger!=null)
							t.printStackTrace(logger);
						else
							t.printStackTrace();
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
	System.out.println("Starting FileListener for Dataset {"+fileListener.dataset+"} ");
    isDone = true;
  }

public boolean isDone() {
	return isDone;
}
  
	public static File[] getFiles(File directory, IOFileFilter fileFilter) {
//		 File[] files = directory.listFiles(fileFilter);
//		Collection<File> list = FileUtils.listFiles(directory, fileFilter,TrueFileFilter.INSTANCE);
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
	
	
	public static void moveInputFile(File inputFile, long timeStamp, boolean isSuccess) 
	{
		File directory = successDir;
		if(!isSuccess)
			directory = errorDir;
			
		File doneFile = new File(directory,timeStamp+"."+inputFile.getName());
		try {
			FileUtils.moveFile(inputFile, doneFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		File sortedtFile = new File(inputFile.getParent(), FilenameUtils.getBaseName(inputFile.getName())+ "_sorted." + FilenameUtils.getExtension(inputFile.getName()));
		if(sortedtFile.exists())
		{
			File sortedDoneFile = new File(directory,timeStamp+"."+sortedtFile.getName());
			try {
				FileUtils.moveFile(sortedtFile, sortedDoneFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}



}