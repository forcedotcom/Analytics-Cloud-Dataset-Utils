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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.util.CSVReader;
import com.sforce.dataset.util.CsvWriter;
import com.sforce.dataset.util.FileUtilsExt;
import com.sforce.dataset.util.Logger;

/**
 * The Class ErrorWriter.
 */
public class ErrorWriter {
			
	/** The delimiter. */
	private char delimiter = ',';
	
	/** The Header line. */
	private String HeaderLine = "";	 
	
	/** The header columns. */
	private ArrayList<String> headerColumns = null;	 
	
	/** The Header suffix. */
	private String HeaderSuffix = "Error";
	
	/** The error file suffix. */
	public static String errorFileSuffix = "_err.";
	
	/** The error csv. */
	private File errorCsv;
	
	/** The writer. */
	private BufferedWriter fWriter = null;
	
	/** The Constant LF. */
	public static final char LF = '\n';

	/** The Constant CR. */
	public static final char CR = '\r';

	/** The Constant QUOTE. */
	public static final char QUOTE = '"';

	/** The Constant COMMA. */
	public static final char COMMA = ',';

	

	/**
	 * Instantiates a new error writer.
	 *
	 * @param inputCsv the input csv
	 * @param delimiter the delimiter
	 * @param pref the pref
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public ErrorWriter(File inputCsv,char delimiter,Charset inputFileCharset)
			throws IOException 
	{
		if(inputCsv==null|| !inputCsv.exists())
		{
			throw new IOException("inputCsv file {"+inputCsv+"} does not exist");
		}
		
//		if(delimiter == null || delimiter.isEmpty())
//		{
//			delimiter = ",";
//		}

		this.delimiter = delimiter;

//		CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(CodingErrorAction.IGNORE, null)), pref);
//		headerColumns = reader.getHeader(true);		
//		reader.close();
		CSVReader reader = new CSVReader(new FileInputStream(inputCsv),inputFileCharset.name() , delimiter);
		headerColumns = reader.nextRecord();
		reader.finalise();
		
		StringBuffer hdrLine = new StringBuffer();
		hdrLine.append(HeaderSuffix);
		int cnt = 0;
		for(String hdr:headerColumns)
		{
			cnt++;
			hdrLine.append(this.delimiter);			
			hdrLine.append(hdr);
			if(hdrLine.length()>400000)
				break;
			if(cnt>5000)
				break;
		}
		this.HeaderLine = hdrLine.toString();
		
		this.errorCsv = new File(inputCsv.getParent(), FilenameUtils.getBaseName(inputCsv.getName())+ errorFileSuffix + FilenameUtils.getExtension((inputCsv.getName())));
		if(this.errorCsv.exists())
		{
			try
			{
				File archiveDir = new File(inputCsv.getParent(),"archive");
				FileUtils.moveFile(this.errorCsv, new File(archiveDir,this.errorCsv.getName()+"."+this.errorCsv.lastModified()));
			}catch(Throwable t)
			{
				FileUtilsExt.deleteQuietly(this.errorCsv);
			}
		}		
	}
	
	public void addError(List<String> values, String error)
	{
		try 
		{
			if(fWriter == null)
			{
				fWriter = new BufferedWriter(new FileWriter(this.errorCsv),DatasetUtilConstants.DEFAULT_BUFFER_SIZE);
				if(this.HeaderLine!=null)
					fWriter.write(this.HeaderLine+"\n");
			}
			if(error==null)
				error="null";
			fWriter.write(getCSVFriendlyString(error));
			if(values!=null)
			{
				for(String val:values)
				{
					fWriter.write(this.delimiter);	
					if(val!=null)
						fWriter.write(CsvWriter.encode(val, this.delimiter , QUOTE));
				}
			}
			fWriter.write("\n");
		} catch (Throwable t) {
			t.printStackTrace(Logger.out);
		}
	}
	
	
	/**
	 * Gets the error file.
	 *
	 * @return the error file
	 */
	public File getErrorFile() 
	{
		return this.errorCsv;
	}
	
	/**
	 * Finish.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void finish() throws IOException
	{
		if (fWriter != null) {
			fWriter.flush();
			IOUtils.closeQuietly(fWriter);
		}
		fWriter = null;
	}
	
	/**
	 * Gets the CSV friendly string.
	 *
	 * @param content the content
	 * @return the CSV friendly string
	 */
	public static String getCSVFriendlyString(String content)
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
	public static String replaceString(String original, String pattern, String replace) 
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
}
