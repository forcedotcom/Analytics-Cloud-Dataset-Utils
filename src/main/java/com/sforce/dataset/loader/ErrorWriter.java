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
import java.io.InputStreamReader;
import java.nio.charset.CodingErrorAction;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.encoder.DefaultCsvEncoder;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import com.sforce.dataset.util.DatasetUtils;

public class ErrorWriter {
			
	private String delimiter = ",";
	private String HeaderLine = "";	 
	private String[] headerColumns = null;	 
	private String HeaderSuffix = "Error";
	private String errorFileSuffix = "_err.";
	private File errorCsv;
	private BufferedWriter fWriter = null;
	private CsvContext context = new CsvContext(1, 1, 1);
	private CsvPreference preference = CsvPreference.STANDARD_PREFERENCE;
	private DefaultCsvEncoder csvEncoder = new DefaultCsvEncoder();

	public static final char LF = '\n';

	public static final char CR = '\r';

	public static final char QUOTE = '"';

	public static final char COMMA = ',';

	

	public ErrorWriter(File inputCsv,String delimiter)
			throws IOException 
	{
		if(inputCsv==null|| !inputCsv.exists())
		{
			throw new IOException("inputCsv file {"+inputCsv+"} does not exist");
		}
		
		if(delimiter == null || delimiter.isEmpty())
		{
			delimiter = ",";
		}

		this.delimiter = delimiter;

		CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(CodingErrorAction.IGNORE, null)), CsvPreference.STANDARD_PREFERENCE);
		headerColumns = reader.getHeader(true);		
		reader.close();
		
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
				FileUtils.deleteQuietly(this.errorCsv);
			}
		}		
	}
	
	public void addError(String[] values, String error)
	{
		try 
		{
			if(fWriter == null)
			{
				fWriter = new BufferedWriter(new FileWriter(this.errorCsv));
					fWriter.write(this.HeaderLine+"\n");
			}
			fWriter.write(getCSVFriendlyString(error));
			for(String val:values)
			{
				fWriter.write(this.delimiter);			
				fWriter.write(csvEncoder.encode(val, context, preference));
			}
			fWriter.write("\n");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
		
	public File getErrorFile() 
	{
		return this.errorCsv;
	}
	
	public void finish() throws IOException
	{
		if (fWriter != null) {
			fWriter.flush();
			fWriter.close();
		}
		fWriter = null;
	}
	
	/**
	 * This method will return a sanitized version of the string that can be safely written to a csv file
	 * It will strip all (commas, CR, CRLF and quotes) from the string
	 * @param content the string that needs to be cleaned
	 * @return the clean string
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
	 * @param original String
	 * @param pattern String to Search
	 * @param replace String to replace
	 * @return
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
