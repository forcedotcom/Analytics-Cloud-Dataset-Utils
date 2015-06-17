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
package com.sforce.dataset.loader.file.schema.ext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.sforce.dataset.util.DatasetUtils;


public class DetectFieldTypes {

	public static final int sampleSize = 3000;
//	public static final Pattern dates = Pattern.compile("(.*)([0-9]{1,2}[/-\\\\.][0-9]{1,2}[/-\\\\.][0-9]{4}|[0-9]{4}[/-\\\\.][0-9]{1,2}[/-\\\\.][0-9]{1,2}|[0-9]{1,2}[/-\\\\.][0-9]{1,2}[/-\\\\.][0-9]{1,2}|[0-9]{1,2}[/-\\\\.][A-Z]{3}[/-\\\\.][0-9]{4}|[0-9]{4}[/-\\\\.][A-Z]{3}[/-\\\\.][0-9]{1,2}|[0-9]{1,2}[/-\\\\.][A-Z]{3}[/-\\\\.][0-9]{1,2})(.*)");
//	public static final Pattern numbers = Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");	
//	public static final Pattern text = Pattern.compile("^[a-zA-z0-9]*$");
	public static final String[] additionalDatePatterns  = {"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","yyyy-MM-dd'T'HH:mm:ss'Z'","yyyy-MM-dd'T'HH:mm:ss.SSS","yyyy-MM-dd'T'HH:mm:ss","MM/dd/yyyy HH:mm:ss","MM/dd/yy HH:mm:ss","MM-dd-yyyy HH:mm:ss","MM-dd-yy HH:mm:ss","dd/MM/yyyy HH:mm:ss","dd/MM/yy HH:mm:ss","dd-MM-yyyy HH:mm:ss","dd-MM-yy HH:mm:ss","MM/dd/yyyy","MM/dd/yy","dd/MM/yy","dd/MM/yyyy","MM-dd-yyyy","MM-dd-yy","dd-MM-yyyy","dd-MM-yy","M/d/yyyy HH:mm:ss","M/d/yy HH:mm:ss","M-d-yyyy HH:mm:ss","M-d-yy HH:mm:ss","d/M/yyyy HH:mm:ss","d/M/yy HH:mm:ss","d-M-yyyy HH:mm:ss","d-M-yy HH:mm:ss","M/d/yy","M/d/yyyy","d/M/yy","d/M/yyyy","M-d-yy","M-d-yyyy","d-M-yy","d-M-yyyy","M/dd/yyyy HH:mm:ss","M/dd/yy HH:mm:ss","M-dd-yyyy HH:mm:ss","M-dd-yy HH:mm:ss","dd/M/yyyy HH:mm:ss","dd/M/yy HH:mm:ss","dd-M-yyyy HH:mm:ss","dd-M-yy HH:mm:ss","M/dd/yy","dd/M/yy","M-dd-yy","dd-M-yy","M/dd/yyyy","dd/M/yyyy","M-dd-yyyy","dd-M-yyyy","MM/d/yyyy HH:mm:ss","MM/d/yy HH:mm:ss","MM-d-yyyy HH:mm:ss","MM-d-yy HH:mm:ss","d/MM/yyyy HH:mm:ss","d/MM/yy HH:mm:ss","d-MM-yyyy HH:mm:ss","d-MM-yy HH:mm:ss","MM/d/yy","d/MM/yy","MM-d-yy","d-MM-yy","MM/d/yyyy","d/MM/yyyy","MM-d-yyyy","d-MM-yyyy"};
	
	public List<FieldType> detect(File inputCsv, ExternalFileSchema userSchema, Charset fileCharset, CsvPreference pref, PrintStream logger) throws IOException
	{
		CsvListReader reader = null;
		LinkedList<FieldType> types = null;
		String[] header = null;
		
		if(userSchema!=null && userSchema.getFileFormat() != null && userSchema.getFileFormat().getNumberOfLinesToIgnore()==0)
		{
			LinkedList<ObjectType> obj = userSchema.getObjects();
			if(obj!= null && !obj.isEmpty())
			{
				List<FieldType> fields = obj.get(0).getFields();
				if(fields!= null && !fields.isEmpty())
				{
					return fields;
				}
			}
		}
		
		try 
		{
			reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , fileCharset)), pref);
			header = reader.getHeader(true);
			
			if(reader!=null)
			{
				reader.close();
				reader = null;
			}
			
			if(userSchema!=null && userSchema.getFileFormat() != null && userSchema.getFileFormat().getNumberOfLinesToIgnore()>0)
			{
				LinkedList<ObjectType> obj = userSchema.getObjects();
				if(obj!= null && !obj.isEmpty())
				{
					List<FieldType> fields = obj.get(0).getFields();
					if(fields!= null && !fields.isEmpty())
					{
						int fieldCount = 0;
						for(FieldType field:fields)
						{
							if(!field.isComputedField)
								fieldCount++;
						}
						
						if(header.length!=fieldCount)
						{
							throw new IllegalArgumentException("Input file header count {"+header.length+"} does not match json field count {"+fieldCount+"}");
						}
						
						return fields;
					}
				}
			}


			List<String> nextLine = null;
			types = new LinkedList<FieldType>();
			boolean uniqueColumnFound = false;
			
			if(header==null)
				return types;
			
			if(header.length>5000)
			{
				throw new IllegalArgumentException("Input file cannot contain more than 5000 columns. found {"+header.length+"} columns");
			}

			String devNames[] = ExternalFileSchema.createUniqueDevName(header);
			boolean first = true;
			for (int i=0; i< header.length; i++) 
			{
				if(i==0)
				{
					if(header[i] != null && header[i].startsWith("#"))
						header[i] = header[i].replace("#", "");
				}

//				boolean found = false;
//				if(userSchema != null)
//				{						
//					LinkedList<ObjectType> obj = userSchema.getObjects();
//					if(obj!= null && !obj.isEmpty())
//					{
//						List<FieldType> fields = obj.get(0).getFields();
//						if(fields!= null && !fields.isEmpty())
//						{							
//							for(FieldType field:fields)
//							{
//								if(field.getName().equals(devNames[i]) || field.getFullyQualifiedName().equals(devNames[i]))
//								{
//									types.add(field);
//									found = true;
//									break;
//								}
//							}
//						}
//					}
//				}
//				if(found)
//					continue;
				
				if(first)
				{
					logger.println("Detecting schema from csv file {"+ inputCsv +"} ...");
					first = false;
				}

				
				LinkedList<String> columnValues = new LinkedList<String>();			
				HashSet<String> uniqueColumnValues = new HashSet<String>();			
				int rowCount = 0;
				logger.print("Column: "+ header[i]);
				try
				{
					reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , fileCharset)), pref);
					header = reader.getHeader(true);

					rowCount++;
					while ((nextLine = reader.read()) != null) {
						rowCount++;
						if(i>=nextLine.size())
							continue; //This line does not have enough columns
						if(nextLine.get(i) != null && !nextLine.get(i).trim().isEmpty())
						{
							columnValues.add(nextLine.get(i).trim());
							uniqueColumnValues.add(nextLine.get(i).trim());
						}
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
				logger.print(", ");
				FieldType newField = null;
				int prec = detectTextPrecision(uniqueColumnValues);
				DecimalFormat df = null;
				BigDecimal bd = detectNumeric(columnValues, null);
				if(bd==null)
				{
					 df = (DecimalFormat) NumberFormat.getInstance(Locale.getDefault());
					 df.setParseBigDecimal(true);
					 bd = detectNumeric(columnValues, df);
				}
				if(bd==null)
				{
					 df = (DecimalFormat) NumberFormat.getCurrencyInstance();
					 df.setParseBigDecimal(true);
					 bd = detectNumeric(columnValues, df);
				}
				if(bd!=null && (uniqueColumnValues.size() == (rowCount-1)) && bd.scale() == 0 && !uniqueColumnFound)
				{	
					bd = null; //this is a Numeric uniqueId therefore treat is Text/Dim
				}
				
				if(bd!=null)
				{
						newField = FieldType.GetMeasureKeyDataType(devNames[i], 0, bd.scale(), 0L);
						if(df!=null)
						{
							newField.setDecimalSeparator(df.getDecimalFormatSymbols().getDecimalSeparator()+"");
							String format = df.toPattern().replace("¤", df.getDecimalFormatSymbols().getCurrencySymbol());
							if(isPercent(columnValues))
							{
								format = format + df.getDecimalFormatSymbols().getPercent();
							}
							newField.setFormat(format);
						}
						logger.println("Type: Numeric, Scale: "+ bd.scale());
				}else
				{
					SimpleDateFormat sdf = null;
						sdf = detectDate(columnValues);
					if(sdf!= null)
					{
						newField =  FieldType.GetDateKeyDataType(devNames[i], sdf.toPattern(), null);
						logger.println("Type: Date, Format: "+ sdf.toPattern());
					}else
					{
						newField =  FieldType.GetStringKeyDataType(devNames[i], null, null);
						if(!uniqueColumnFound && uniqueColumnValues.size() == (rowCount-1) && prec<32)
						{
							newField.isUniqueId = true;
							uniqueColumnFound = true;
						}
						if(prec>255)
						{
							logger.println("Type: Text, Precison: "+255+" (Column will be truncated to 255 characters)" + (newField.isUniqueId? ", isUniqueId=true":""));
						}
						else
						{
							logger.println("Type: Text, Precison: "+prec + (newField.isUniqueId? ", isUniqueId=true":""));
						}
						newField.setPrecision(255); //Assume upper limit for precision of text fields even if the values may be smaller
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
				logger.println("Schema file {"+ ExternalFileSchema.getSchemaFile(inputCsv, logger) +"} successfully generated...");
			}

		} finally  {
			logger.println("");
			if(reader!=null)
					reader.close();
		}
		return types;
	}
	
	public BigDecimal detectNumeric(LinkedList<String> columnValues, DecimalFormat df) 
	{
        BigDecimal maxScale  = null;
        BigDecimal maxPrecision  = null;
	    int consectiveFailures = 0;
	    int success = 0;
	    int absoluteMaxScale = 6;
	    int absoluteMaxScaleExceededCount = 0;
	    int absoluteMaxPrecision = 18;

	    //Date dt = new Date(System.currentTimeMillis());
	    for(int j=0;j<columnValues.size();j++)
	    {
	        String columnValue = columnValues.get(j);
	    	if(columnValue == null || columnValue.isEmpty())
	    		continue;
	    	
	    	if(columnValue.length()>absoluteMaxPrecision)
	    		continue;

	        BigDecimal bd = null;
	    	try
	         {
	    		if(df==null)
	    			bd = new BigDecimal(columnValue);
	    		else
	    			bd = (BigDecimal) df.parse(columnValue);
	    		
	    		 if(bd.scale()>absoluteMaxScale)
	    			 absoluteMaxScaleExceededCount++;
	    		 
	 	    	 if(bd.precision()>absoluteMaxPrecision || bd.scale()>absoluteMaxPrecision)
		    		continue;
	 	    	 
	    		 //logger.println("Value: {"+columnValue+"} Scale: {"+bd.scale()+"}");
	    		 if(maxScale == null || bd.scale() > maxScale.scale())
	    			 maxScale = bd;
	    		 
	    		 if(maxPrecision == null || bd.precision() > maxPrecision.precision())
	    			 maxPrecision = bd;
	    		 
				success++;
				consectiveFailures=0; //reset the failure count
	         }catch(Throwable t)
	         {
	        	 consectiveFailures++;
	         }
	    	
	    	if(consectiveFailures>=1000)
	    	{
	    		
	    		return null;
	    	}
	    }
	    
	    if(maxScale==null || maxPrecision==null)
	    	return null;

	    //if more than 10% values are going to be over the max scale then we are better of treating this as Text
	    if((1.0*absoluteMaxScaleExceededCount/columnValues.size()) > 0.1)
	    {
	    	return null;
	    }
	    
	    int maxScaleCalculated = absoluteMaxScale;
	    if(maxPrecision!=null && maxPrecision.precision()>maxPrecision.scale())
	    {
	        maxScaleCalculated = absoluteMaxPrecision - (maxPrecision.precision()-maxPrecision.scale());
	    	if(maxScaleCalculated>absoluteMaxScale)
	    		maxScaleCalculated = absoluteMaxScale;
	    	else if(maxScaleCalculated<2)
	    		maxScaleCalculated=2;
	    }

	    
	    if(maxScale!=null && maxScale.scale()>maxScaleCalculated)
	    {
	    	maxScale = maxScale.setScale(maxScaleCalculated, RoundingMode.HALF_EVEN);
	    }
	    
	    maxPrecision = maxPrecision.setScale(maxScale.scale(), RoundingMode.HALF_EVEN);

	    if((1.0*success/columnValues.size()) > 0.95)
	    {
	    	return maxPrecision;
	    }else
	    {
	    	return null;
	    }
	}
	
	public SimpleDateFormat detectDate(LinkedList<String> columnValues) 
	{
	    
		LinkedHashSet<SimpleDateFormat> dateFormats = getSuportedDateFormats();

	    for(int j=0;j<columnValues.size();j++)
	    {
	        String columnValue = columnValues.get(j);
	         Date dt = null;
	         SimpleDateFormat dtf = null;
	    	if(columnValue == null || columnValue.isEmpty())
	    		continue;
	    	
	    	if(columnValue.length()<6 || columnValue.length()>30)
	    		continue;

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
//							 logger.println(i + "| " + locales[i].getDisplayCountry()+"| "+dtf.toPattern());
//							 logger.println(columnValue.trim());
//							 t.printStackTrace();
//				        }
		        	 dtf = null;
		        	 dt = null;
		         }
		      }

	    	 if(dt!=null)
	    	 {
	    		    int consectiveFailures = 0;
	    		    int success = 0;
	    		    for(int k=0;k<columnValues.size();k++)
	    		    {
	    		        columnValue = columnValues.get(k);
	    		    	if(columnValue == null || columnValue.isEmpty())
	    		    		continue;
	    		    	
			        	 try {
							Date dt1 = dtf.parse(columnValue);
				        	 String tmpDate = dtf.format(dt1);
				        	 if(tmpDate.length() == columnValue.length())
				        	 {
				        		success++;
								consectiveFailures=0; //reset the failure count
				        	 }
						} catch (ParseException e) {
							consectiveFailures++;
						}

			        	 if(consectiveFailures>=1000)
				    		break;
	    		    }
	    		    if(!(consectiveFailures>=1000) && (1.0*success/columnValues.size()) > 0.95)
	    		    {
	    		    	return dtf;
	    		    }else
	    		    {
	    		    	dateFormats.remove(dtf); //lets not try this format again
			        	 dtf = null;
			        	 dt = null;
	    		    }
	    	 }
	    }
		return null;
	    
	}
	
	public int detectTextPrecision(HashSet<String> uniqueColumnValues) {
		int length = 0;
	    for(String columnValue:uniqueColumnValues)
	    {
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
	      SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
	      tempSdf.setLenient(false);
	      dateFormats.add(tempSdf);
	    }

	    for (int i = 0; i < additionalDatePatterns.length; i++) 
	    {
	         try
	         {
	        	 SimpleDateFormat sdf = new SimpleDateFormat(additionalDatePatterns[i]);
		   	     SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
			     tempSdf.setLenient(false);
			     dateFormats.add(tempSdf);
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
	   	    SimpleDateFormat tempSdf = new SimpleDateFormat(sdf.toPattern());
		    tempSdf.setLenient(false);
		    dateFormats.add(tempSdf);
	    }
	    return dateFormats;
    }
	
	public boolean isPercent(LinkedList<String> columnValues) 
	{
	    int success = 0;
	    for(int j=0;j<columnValues.size();j++)
	    {	
	    	if(columnValues.get(j).contains("%"))
	    	{
	    		success++;
	    	}
	    }
	    if((1.0*success/columnValues.size()) > 0.95)
	    {
	    	return true;
	    }
	    return false;
	}
	


	    


}
