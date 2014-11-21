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

import java.util.concurrent.BlockingQueue;
 
public class WriterThread implements Runnable {
	
  private static final int max_error_threshhold = 10000;

  private final BlockingQueue<String[]> queue;
  private final EbinFormatWriter w;
  private final ErrorWriter ew;

  private volatile boolean isDone = false;
  private volatile int errorRowCount = 0;
  private volatile int totalRowCount = 0;


WriterThread(BlockingQueue<String[]> q,EbinFormatWriter w,ErrorWriter ew) 
  { 
	  if(q==null || w == null || ew == null)
	  {
		  throw new IllegalArgumentException("Constrictor input cannot be null");
	  }
	  queue = q; 
	  this.w = w;
	  this.ew = ew;
  }
 
  public void run() {
    try {
       
    	System.out.println("Start: " + Thread.currentThread().getName());
       
       String[] row = queue.take();
       while (row != null && row.length!=0) {
			try
			{
					totalRowCount++;
					w.addrow(row);
			}catch(Throwable t)
			{
				if(errorRowCount==0)
				{
					System.err.println();
				}
				System.err.println("Row {"+totalRowCount+"} has error {"+t+"}");
				if(row!=null)
				{
					ew.addError(row, t.getMessage());
					errorRowCount++;
					if(errorRowCount>=max_error_threshhold)
					{
						System.err.println("Max error threshold reached. Aborting processing");
						break;
					}
				}
				//t.printStackTrace();
			}
         row = queue.take();
       }
    }catch (Throwable t) {
       System.out.println (Thread.currentThread().getName() + " " + t.getMessage());
    }
    isDone = true;
	System.out.println("END: " + Thread.currentThread().getName());
  }

public boolean isDone() {
	return isDone;
}

public void setDone(boolean isDone) {
	this.isDone = isDone;
}

public int getErrorRowCount() {
	return errorRowCount;
}

public void setErrorRowCount(int errorRowCount) {
	this.errorRowCount = errorRowCount;
}
  
  public int getTotalRowCount() {
	return totalRowCount;
}

public void setTotalRowCount(int totalRowCount) {
	this.totalRowCount = totalRowCount;
}


}