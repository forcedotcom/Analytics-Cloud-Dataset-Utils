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

import java.io.File;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
 
public class FilePartsUploaderThread implements Runnable {
	

  private final BlockingQueue<Map<Integer,File>> queue;
  private final PartnerConnection partnerConnection;
  private final String insightsExternalDataId;

  private volatile AtomicBoolean done = new AtomicBoolean(false);

  private volatile int errorRowCount = 0;
  private volatile int totalRowCount = 0;
  private final PrintStream logger;

	public static final NumberFormat nf = NumberFormat.getIntegerInstance();

FilePartsUploaderThread(BlockingQueue<Map<Integer,File>> q,PartnerConnection partnerConnection, String insightsExternalDataId, PrintStream logger) 
  { 
	  if(partnerConnection==null || insightsExternalDataId == null || q == null)
	  {
		  throw new IllegalArgumentException("Constructor input cannot be null");
	  }
	  queue = q; 
	  this.partnerConnection = partnerConnection;
	  this.insightsExternalDataId = insightsExternalDataId;
	  this.logger = logger;
  }
 
  public void run() {
    try {
       Map<Integer, File> row = queue.take();
   		logger.println("Start: " + Thread.currentThread().getName());
   		done.set(false);
       while (!row.isEmpty()) {
			try
			{
					totalRowCount++;
					if(!insertFileParts(partnerConnection, insightsExternalDataId, row, 0))
						errorRowCount++;
			}catch(Throwable t)
			{
				errorRowCount++;
				t.printStackTrace();
			}
         row = queue.take();
       }
    }catch (Throwable t) {
       logger.println (Thread.currentThread().getName() + " " + t);
    }
    done.set(true);
	logger.println("END: " + Thread.currentThread().getName());
  }

public boolean isDone() {
	return done.get();
}

public int getErrorRowCount() {
	return errorRowCount;
}
  
  public int getTotalRowCount() {
	return totalRowCount;
}
  
  
	private boolean insertFileParts(PartnerConnection partnerConnection, String insightsExternalDataId, Map<Integer,File> fileParts, int retryCount) throws Exception 
	{
		for(int i:fileParts.keySet())
		{
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
	    				return true;
	    			}else
	    			{
						logger.println("File Part {"+ fileParts.get(i) + "} Insert Failed: " + (DatasetLoader.getErrorMessage(sv.getErrors())));
	    			}
	    		}
			} catch (Throwable t) {
				t.printStackTrace();
				logger.println("File Part {"+ fileParts.get(i) + "} Insert Failed: " + t.toString());
			}
		}
//			if(retryCount<3)
//			{		
//				retryCount++;
//				Thread.sleep(1000*retryCount);
////				partnerConnection = DatasetUtils.login(0, username, password, token, endpoint, sessionId);
//				return insertFileParts(partnerConnection, insightsExternalDataId, fileParts, retryCount);
//			}else
			{
				return false;
			}
	}


}