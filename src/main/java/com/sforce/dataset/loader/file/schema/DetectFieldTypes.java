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
package com.sforce.dataset.loader.file.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.sforce.dataset.util.DatasetUtils;


public class DetectFieldTypes {

	public static final int sampleSize = 1000;
//	public static final Pattern dates = Pattern.compile("(.*)([0-9]{1,2}[/-\\\\.][0-9]{1,2}[/-\\\\.][0-9]{4}|[0-9]{4}[/-\\\\.][0-9]{1,2}[/-\\\\.][0-9]{1,2}|[0-9]{1,2}[/-\\\\.][0-9]{1,2}[/-\\\\.][0-9]{1,2}|[0-9]{1,2}[/-\\\\.][A-Z]{3}[/-\\\\.][0-9]{4}|[0-9]{4}[/-\\\\.][A-Z]{3}[/-\\\\.][0-9]{1,2}|[0-9]{1,2}[/-\\\\.][A-Z]{3}[/-\\\\.][0-9]{1,2})(.*)");
//	public static final Pattern numbers = Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");	
//	public static final Pattern text = Pattern.compile("^[a-zA-z0-9]*$");
	public static final String[] additionalDatePatterns ={"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","yyyy-MM-dd'T'HH:mm:ss'Z'","yyyy-MM-dd'T'HH:mm:ss.SSS","yyyy-MM-dd'T'HH:mm:ss","M/d/yyyy HH:mm:ss","M/d/yy HH:mm:ss","M-d-yyyy HH:mm:ss","M-d-yy HH:mm:ss","d/M/yyyy HH:mm:ss","d/M/yy HH:mm:ss","d-M-yyyy HH:mm:ss","d-M-yy HH:mm:ss", "M/d/yy", "d/M/yy","M-d-yy", "d-M-yy"}; //
	
	
	public LinkedList<FieldType> detect(File inputCsv, ExternalFileSchema userSchema, Charset fileCharset) throws IOException
	{
		CsvListReader reader = null;
		LinkedList<FieldType> types = null;
		String[] header = null;
		try 
		{
			reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , fileCharset)), CsvPreference.STANDARD_PREFERENCE);
			header = reader.getHeader(true);

			if(reader!=null)
			{
				reader.close();
				reader = null;
			}
			
			List<String> nextLine = null;
			types = new LinkedList<FieldType>();
			String devNames[] = ExternalFileSchema.createUniqueDevName(header);
			boolean first = true;
			for (int i=0; i< header.length; i++) 
			{
				if(i==0)
				{
					if(header[i] != null && header[i].startsWith("#"))
						header[i] = header[i].replace("#", "");
				}
				boolean found = false;
				if(userSchema != null)
				{					
					LinkedList<ObjectType> obj = userSchema.objects;
					if(obj!= null && !obj.isEmpty())
					{
						List<FieldType> fields = obj.get(0).fields;
						if(fields!= null && !fields.isEmpty())
						{
							for(FieldType field:fields)
							{
								if(field.getName().equals(devNames[i]))
								{
									types.add(field);
									found = true;
									break;
								}
							}
						}
					}
				}
				if(found)
					continue;
				
				if(first)
				{
					System.out.println("Detecting schema from csv file {"+ inputCsv +"} ...");
					first = false;
				}

				
				LinkedList<String> columnValues = new LinkedList<String>();			
				int rowCount = 0;
				System.out.print("Column: "+ header[i]);
				try
				{
					reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , fileCharset)), CsvPreference.STANDARD_PREFERENCE);
					header = reader.getHeader(true);

					rowCount++;
					while ((nextLine = reader.read()) != null) {
						rowCount++;
						if(i>=nextLine.size())
							continue; //This line does not have enough columns
						if(nextLine.get(i) != null && !nextLine.get(i).trim().isEmpty())
							columnValues.add(nextLine.get(i).trim());					
						if(columnValues.size()>=sampleSize || rowCount > 10000)
						{
							break;
						}
					}
				}catch(Throwable t)
				{
					t.printStackTrace();
				}finally
				{
					if(reader!=null)
							reader.close();
					reader = null;
				}
				FieldType newField = null;
				BigDecimal bd = detectNumeric(columnValues);
				if(bd!=null)
				{
						newField = FieldType.GetMeasureKeyDataType(devNames[i], 0, bd.scale(), 0L);
						System.out.println(", Type: Numeric, Scale: "+ bd.scale());
				}else
				{
					SimpleDateFormat sdf = detectDate(columnValues);
					if(sdf!= null)
					{
						newField =  FieldType.GetDateKeyDataType(devNames[i], sdf.toPattern(), null);
						System.out.println(", Type: Date, Format: "+ sdf.toPattern());
					}else
					{
						newField =  FieldType.GetStringKeyDataType(devNames[i], null, null);
						int prec = detectTextPrecision(columnValues);
						if(prec>255)
							System.out.println(", Type: Text, Precison: "+255+" (Column will be truncated to 255 characters)");
						else
							System.out.println(", Type: Text, Precison: "+prec);
					}
				}
				if(newField!=null)
				{
					if(header[i]!=null && !header[i].trim().isEmpty())
					{	
						newField.setLabel(header[i]);
						newField.setDescription(header[i]);
					}
					types.add(newField);
				}
			}//end for

			if(!first)
			{
				System.out.println("Schema file {"+ ExternalFileSchema.getSchemaFile(inputCsv) +"} successfully generated...");
			}

		} finally  {
			System.out.println("");
			if(reader!=null)
					reader.close();
		}
		return types;
	}
	
	public BigDecimal detectNumeric(LinkedList<String> columnValues) 
	{
        BigDecimal maxScale  = null;
	    @SuppressWarnings("unused")
		int failures = 0;
	    int success = 0;

	    //Date dt = new Date(System.currentTimeMillis());
	    for(int j=0;j<columnValues.size();j++)
	    {
	        String columnValue = columnValues.get(j);
	        BigDecimal bd = null;
	    	if(columnValue == null || columnValue.trim().isEmpty())
	    		continue;
	    	else
	    		columnValue = columnValue.trim();

	         try
	         {
	    		 bd = new BigDecimal(columnValue);
	    		 //System.out.println("Value: {"+columnValue+"} Scale: {"+bd.scale()+"}");
	    		 if(maxScale == null || bd.scale() > maxScale.scale())
	    			 maxScale = bd;
				success++;
	         }catch(Throwable t)
	         {
					failures++;
	         }
	    }
	    
	    if(maxScale!=null && maxScale.scale()>9)
	    {
	    	maxScale = maxScale.setScale(9, RoundingMode.HALF_EVEN);
	    }

	    if((1.0*success/columnValues.size()) > 0.85)
	    {
	    	return maxScale;
	    }
	    else
	    {
	    	return null;
	    }
	}
	
	public SimpleDateFormat detectDate(LinkedList<String> columnValues) 
	{
	    @SuppressWarnings("unused")
		int failures = 0;
	    int success = 0;
	    
		LinkedHashSet<SimpleDateFormat> dateFormats = getSuportedDateFormats();

	    for(int j=0;j<columnValues.size();j++)
	    {
	        String columnValue = columnValues.get(j);
	         Date dt = null;
	         SimpleDateFormat dtf = null;
	    	if(columnValue == null || columnValue.trim().isEmpty())
	    		continue;
	    	else
	    		columnValue = columnValue.trim();

		      for (SimpleDateFormat sdf:dateFormats) 
		      {
		         try
		         {
		        	 dt = sdf.parse(columnValue);
		        	 String tmpDate = sdf.format(dt);
		        	 if(tmpDate.length() == columnValue.length())
		        	 {
		        		 dtf = sdf;
		        		 break;
		        	 }else
		        		 dt=null;
		         }catch(Throwable t)
		         {
//				        if(dtf.toPattern().equals("MM/dd/yyyy hh:mm:ss a"))
//				        {
//							 System.out.println(i + "| " + locales[i].getDisplayCountry()+"| "+dtf.toPattern());
//							 System.out.println(columnValue.trim());
//							 t.printStackTrace();
//				        }
		         }
		      }

	    	 if(dt!=null)
	    	 {
	    		    for(int k=0;k<columnValues.size();k++)
	    		    {
	    		        columnValue = columnValues.get(k);
	    		    	if(columnValue == null || columnValue.trim().isEmpty())
	    		    		continue;
	    		    	else
	    		    		columnValue = columnValue.trim();
	    		    	
			        	 try {
							Date dt1 = dtf.parse(columnValue);
				        	 String tmpDate = dtf.format(dt1);
				        	 if(tmpDate.length() == columnValue.length())
				        	 {
				        		 success++;
				        	 }
						} catch (ParseException e) {
							failures++;
						}	    		    	
	    		    }
	    		    if((1.0*success/columnValues.size()) > 0.85)
	    		    {
	    		    	return dtf;
	    		    }else
	    		    {
	    		    	dateFormats.remove(dtf); //lets not try this format again
	    		    }
	    	 }
	    }
		return null;
	    
	}
	
	public int detectTextPrecision(LinkedList<String> columnValues) {
		int length = 0;
	    for(int j=0;j<columnValues.size();j++)
	    {
	        String columnValue = columnValues.get(j);
	        if(columnValue!=null)
	        {
	        	if(columnValue.length()>length)
	        		length = columnValue.length();
	        }
	    }
	    return length;
	}
	
	public LinkedHashSet<SimpleDateFormat> getSuportedDateFormats() 
	{
		LinkedHashSet<SimpleDateFormat> dateFormats = new LinkedHashSet<SimpleDateFormat>();
		Locale[] locales = Locale.getAvailableLocales();
	    for (int i = 0; i < locales.length; i++) 
	    {
	        if (locales[i].getCountry().length() == 0) {
	      	  continue; // Skip language-only locales
	        }	    	  
	      SimpleDateFormat sdf = (SimpleDateFormat) SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.MEDIUM,SimpleDateFormat.MEDIUM, locales[i]);
	      dateFormats.add(new SimpleDateFormat(sdf.toPattern()));
	    }

	    for (int i = 0; i < additionalDatePatterns.length; i++) 
	    {
	         try
	         {
	        	 SimpleDateFormat sdf = new SimpleDateFormat(additionalDatePatterns[i]);
	   	      dateFormats.add(new SimpleDateFormat(sdf.toPattern()));
	         }catch(Throwable t1)
	         {
	        	 t1.printStackTrace();
	         }			    	  
	    }	    

	    for (int i = 0; i < locales.length; i++) 
	    {
	        if (locales[i].getCountry().length() == 0) {
	      	  continue; // Skip language-only locales
	        }	    	  
	      SimpleDateFormat sdf = (SimpleDateFormat) SimpleDateFormat.getDateInstance(SimpleDateFormat.MEDIUM, locales[i]);
	      dateFormats.add(new SimpleDateFormat(sdf.toPattern()));
	    }
	    return dateFormats;
    }

	    


}

