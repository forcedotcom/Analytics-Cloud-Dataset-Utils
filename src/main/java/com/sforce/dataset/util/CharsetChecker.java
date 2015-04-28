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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;

public class CharsetChecker {

//	private static final int DEFAULT_BUFFER_SIZE = 8*1024*1024;
//	private static final int EOF = -1;
	
	public static Charset detectCharset(File inputFile, PrintStream logger)
	{
		CharsetDetector detector = null;
	    InputStream input = null;
		try {
		    detector = new CharsetDetector();
			input = new BufferedInputStream(new FileInputStream(inputFile));
		    detector.setText(input);
//			input = new FileInputStream(inputFile);
//			byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
//		    detector = new CharsetDetector();
//            Arrays.fill(buffer, (byte)0);
//			int n = 0;
//			while (EOF != (n = input.read(buffer))) {
//				detector.setText(buffer);
//			}
		    CharsetMatch cm = detector.detect();
			logger.println("\n*******************************************************************************");					
		    logger.println("File {"+inputFile+"} has Charset {"+cm.getName()+"}, Confidence: {"+cm.getConfidence()+"%}");
			logger.println("*******************************************************************************\n");					
		    
		    return Charset.forName(cm.getName());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally
		{
			if(input!=null)
			{
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return Charset.forName("UTF-8");
	}
}
