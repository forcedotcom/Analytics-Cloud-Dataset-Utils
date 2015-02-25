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

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.monitor.Session;
 
public class WriterThread implements Runnable {
	
  private static final int max_error_threshhold = 10000;

  private final BlockingQueue<String[]> queue;
  @SuppressWarnings("deprecation")
private final EbinFormatWriter ebinWriter;
  private final ErrorWriter errorwriter;
  private final PrintStream logger;

  private volatile AtomicBoolean done = new AtomicBoolean(false);
  private volatile AtomicBoolean aborted = new AtomicBoolean(false);
  private volatile int errorRowCount = 0;
  private volatile int totalRowCount = 0;
  Session session = null;


@SuppressWarnings("deprecation")
WriterThread(BlockingQueue<String[]> q,EbinFormatWriter w,ErrorWriter ew, PrintStream logger, Session session) 
  { 
	  if(q==null || w == null || ew == null || session == null)
	  {
		  throw new IllegalArgumentException("Constructor input cannot be null");
	  }
	  queue = q; 
	  this.ebinWriter = w;
	  this.errorwriter = ew;
	  this.logger = logger;
	  this.session = session;
  }
 
  @SuppressWarnings("deprecation")
public void run() {
 		logger.println("Start: " + Thread.currentThread().getName());
    try {
       String[] row = queue.take();
       while (row != null && row.length!=0) 
       {
    	   if(session.isDone())
			{
				throw new DatasetLoaderException("Operation terminated on user request");
			}

			try
			{
					totalRowCount++;
					ebinWriter.addrow(row);
			}catch(Throwable t)
			{
				if(errorRowCount==0)
				{
					logger.println();
				}
				logger.println("Row {"+totalRowCount+"} has error {"+t+"}");
				if(row!=null)
				{
					errorRowCount++;
					if(DatasetUtilConstants.debug)
						t.printStackTrace();
					errorwriter.addError(row, t.getMessage()!=null?t.getMessage():t.toString());
					if(errorRowCount>=max_error_threshhold)
					{
						logger.println("Max error threshold reached. Aborting processing");
						aborted.set(true);
						queue.clear();
						break;
					}
				}
				//t.printStackTrace();
			}finally
			{
				if(session!=null)
				{
					session.setTargetTotalRowCount(totalRowCount);
					session.setTargetErrorCount(errorRowCount);
				}
			}
         row = queue.take();
       }
    }catch (Throwable t) {
       logger.println (Thread.currentThread().getName() + " " + t.toString());
		aborted.set(true);
		queue.clear();
    }finally
    {
	    try {
			ebinWriter.finish();
		} catch (IOException e) {
			e.printStackTrace(logger);
		}
	    try {
	    	errorwriter.finish();
		} catch (IOException e) {
			e.printStackTrace(logger);
		}
    }
	logger.println("END: " + Thread.currentThread().getName());
    done.set(true);
	queue.clear();
  }

public boolean isDone() {
	return done.get();
}

public boolean isAborted() {
	return aborted.get();
}

public int getErrorRowCount() {
	return errorRowCount;
}
  
  public int getTotalRowCount() {
	return totalRowCount;
}

}