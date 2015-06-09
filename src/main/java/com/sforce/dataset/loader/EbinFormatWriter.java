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
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.SimpleBindings;

import org.apache.commons.io.IOUtils;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.loader.file.schema.ext.FieldType;
import com.sforce.dataset.util.FiscalDateUtil;

/**
 * @author pgupta
 * 
 * This format has been deprecated and not recommended for use. 
 * Simply upload the file in CSV format 
 *
 */
@Deprecated
public class EbinFormatWriter {
	
	static byte[] delimiter = new byte[] {(byte)0x3A}; // ":"
	static final byte[] magic = new byte[] {(byte)0x11, (byte)0xff, (byte)0x48};
	static final byte version_high = 1;
	static final byte version_low = 0;
	static final byte checksum = 17;
	static final long edgeMinValue = -36028797018963968L;
	static final long edgeMaxValue = 36028797018963967L;
	static final int maxTextLength = 255;

	
	LinkedList<Integer> measure_index = new  LinkedList<Integer>();
	LinkedList<FieldType> _dataTypes = new  LinkedList<FieldType>();
	LinkedHashMap<String,Object> prev = new LinkedHashMap<String,Object>();;

	private int numColumns = 0;	
	private OutputStream out;
	private final PrintStream logger;
	int interval = 10;
	


	private volatile int successRowCount = 0;
	private volatile int totalRowCount = 0;

	Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    DecimalFormat df = new DecimalFormat("00");
    DecimalFormat df4 = new DecimalFormat("0000");

	public static final NumberFormat nf = NumberFormat.getIntegerInstance();
	long startTime = 0L;
	
	public EbinFormatWriter(OutputStream out, List<FieldType> dataTypes,PrintStream logger)
			throws IOException 
	{
		this(out,dataTypes.toArray(new FieldType[0]), logger);
	}

	
	public EbinFormatWriter(OutputStream out, FieldType[] dataTypes,PrintStream logger)
			throws IOException 
	{
		this(out, logger);
//		this.numColumns = dataTypes.length;
		
		if(dataTypes.length>5000)
		{
			throw new IllegalArgumentException("Input file cannot contain more than 5000 columns. Found {"+dataTypes.length+"} columns");
		}

		for (FieldType dataType: dataTypes)
		{
			_dataTypes.add(dataType);
			if(!dataType.isComputedField)
				numColumns++;	

			if(dataType.isSkipped)
			{
				continue;
			}
			
			if (dataType.getfType() == FieldType.DATE)
			{
				_dataTypes.add(FieldType.GetMeasureKeyDataType(dataType.getName() + "_sec_epoch", 0 , 0, 0L));
				_dataTypes.add(FieldType.GetMeasureKeyDataType(dataType.getName() + "_day_epoch", 0 , 0, 0L));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Day", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Month", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Year", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Quarter", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Week", null, null));

				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Hour", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Minute", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Second", null, null));
					
				if(dataType.getFiscalMonthOffset()>0)
				{
					_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Month_Fiscal", null, null));
					_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Year_Fiscal", null, null));
					_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Quarter_Fiscal", null, null));
					_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Week_Fiscal", null, null));
				}
			}			
		}

		this.initmeasures(_dataTypes);
		
	    df.setMinimumIntegerDigits(2);
	}
	
	protected EbinFormatWriter(OutputStream out,PrintStream logger)  throws IOException
	{
		this.out = out;
		this.logger = logger;
		totalRowCount = 0;
		out.write(magic, 0, 3);
		out.write(version_high);
		out.write(version_low);
 	}


	public void addrow(String[] values)  throws IOException,NumberFormatException, ParseException
	{
		LinkedList<Long> measure_values = new LinkedList<Long>();
		LinkedList<String> dim_keys = new LinkedList<String>();
		LinkedList<String> dim_values = new LinkedList<String>();
		LinkedHashMap<String,Object> curr = new LinkedHashMap<String,Object>();
		
		int count = 0;
		int key_value_count = 0;

		totalRowCount++;
		if (values.length != this.numColumns) {
			String message = "Row " + totalRowCount + " contains an invalid number of Values, expected " + 
					this.numColumns + " Value(s), got " + values.length + ".";
			throw new IOException(message);
		}
				
		if(totalRowCount%interval==0||totalRowCount==1)
		{
			long newStartTime = System.currentTimeMillis();
			if(startTime==0)
				startTime = newStartTime;
			logger.println("Processing row {"+nf.format(totalRowCount) +"} time {"+nf.format(newStartTime-startTime)+"}");			
			startTime = newStartTime;
		}
		
		if(interval < 1000000 && totalRowCount/interval>=10)
		{
			interval = interval*10;
		}
		
		while(key_value_count<_dataTypes.size())
		{
//			System.err.println("Count: " + count + " ; key_value_count: " + key_value_count  );
//			System.err.println(_dataTypes.get(key_value_count).getName() + ":" + _dataTypes.get(key_value_count).getType() + " = " + values[count]);
//			System.err.println();

			if(_dataTypes.get(key_value_count).isSkipped)
			{
				key_value_count++;
				count++;
				continue;
			}
			
			Object columnValue = _dataTypes.get(key_value_count).getDefaultValue();
			if(_dataTypes.get(key_value_count).getfType() == FieldType.DATE)
				columnValue =  _dataTypes.get(key_value_count).getDefaultDate();
			
			if(_dataTypes.get(key_value_count).isComputedField)
			{
	            Bindings bindings = new SimpleBindings();
	            CompiledScript cs = _dataTypes.get(key_value_count).getCompiledScript();
	            try
	            {
	            if(cs!=null)
	            {
	            	bindings.put("curr", curr);
	            	bindings.put("prev", prev);
	            	columnValue = cs.eval(bindings);
	            	if(DatasetUtilConstants.debug)
	            	{
	            		logger.println(_dataTypes.get(key_value_count).getName()+ " Computed columnValue: "+columnValue);
	            	}
	            }
	            }catch(Throwable t)
	            {
	            	logger.println("Field {"+_dataTypes.get(key_value_count).getName()+"} has Invalid Expression {"+_dataTypes.get(key_value_count).getComputedFieldExpression()+"}");
	            	t.printStackTrace();
	            }
			}else
			{
				if(values[count]!=null)
				{
					columnValue = values[count];
				}
			}

			if (_dataTypes.get(key_value_count).getfType() == FieldType.MEASURE) 
			{
				try
				{
					BigDecimal v = new BigDecimal(0);
					BigDecimal mv = new BigDecimal(0);
					if(columnValue!=null)
					{
						if(columnValue instanceof Double)
							v = new BigDecimal((Double)columnValue);
						else
							v = new BigDecimal(columnValue.toString());
						
						if (_dataTypes.get(key_value_count).getMeasure_multiplier() > 1)
						{
							mv = v.multiply(_dataTypes.get(key_value_count).getMeasure_multiplier_bd());
						}else
						{
							mv = v;
						}
						if(mv.doubleValue()>edgeMaxValue || mv.doubleValue()<edgeMinValue)
							throw new ParseException("Value {"+mv+"} out of range ("+edgeMinValue+"-"+edgeMaxValue+")",0);
					}
					measure_values.add((long)mv.doubleValue());
					curr.put(_dataTypes.get(key_value_count).getName(), v.doubleValue());
					key_value_count++;
				} catch(Throwable t)
				{
					throw new NumberFormatException(_dataTypes.get(key_value_count).getName() +" is not a valid number, value {" + columnValue +"} : " +t.getMessage());
				}
			}else if (_dataTypes.get(key_value_count).getfType() == FieldType.DATE) 
			{
				try
				{
				int fiscalMonthOffset = _dataTypes.get(key_value_count).getFiscalMonthOffset();
				SimpleDateFormat sdt = _dataTypes.get(key_value_count).getCompiledDateFormat();
				boolean isYearEndFiscalYear = _dataTypes.get(key_value_count).isYearEndFiscalYear;
				int firstDayOfWeek = _dataTypes.get(key_value_count).getFirstDayOfWeek();

				if(columnValue==null)
				{
					//The date is null we don't add null dims
//					dim_values.add("");
//					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), null);
					key_value_count++;

					measure_values.add(0L);
					curr.put(_dataTypes.get(key_value_count).getName(), 0L);
					key_value_count++;

					measure_values.add(0L);
					curr.put(_dataTypes.get(key_value_count).getName(), 0L);
					key_value_count++;

					//Skip the rest
				    //DecimalFormat df = new DecimalFormat("00");
				    //dim_values.add(df.parse(Integer.toString(day)).toString());
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), null);
					key_value_count++;

					//dim_values.add(df.parse(Integer.toString(month)).toString());
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), null);
					key_value_count++;

					//dim_values.add(Integer.toString(year));
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), null);
					key_value_count++;

					//dim_values.add(Integer.toString(quarter));
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), null);
					key_value_count++;

					//dim_values.add(Integer.toString(week));
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), null);
					key_value_count++;	

					//This check is temporary remove when schema supports these new data parts
//					if(DatasetUtilConstants.createNewDateParts)
//					{		
						//dim_values.add(Integer.toString(hour));
						//dim_keys.add(_dataTypes.get(key_value_count).getName());
						curr.put(_dataTypes.get(key_value_count).getName(), null);
						key_value_count++;	
	
						//dim_values.add(Integer.toString(minute));
						//dim_keys.add(_dataTypes.get(key_value_count).getName());
						curr.put(_dataTypes.get(key_value_count).getName(), null);
						key_value_count++;	
	
						//dim_values.add(Integer.toString(second));
						//dim_keys.add(_dataTypes.get(key_value_count).getName());
						curr.put(_dataTypes.get(key_value_count).getName(), null);
						key_value_count++;	
						
						if(fiscalMonthOffset>0)
						{
							//dim_values.add(Integer.toString(month_Fiscal));
							//dim_keys.add(_dataTypes.get(key_value_count).getName()+"_Month_Fiscal");
							curr.put(_dataTypes.get(key_value_count).getName(), null);
							key_value_count++;	
	
							//dim_values.add(Integer.toString(year_Fiscal));
							//dim_keys.add(_dataTypes.get(key_value_count).getName()+"_Year_Fiscal");
							curr.put(_dataTypes.get(key_value_count).getName(), null);
							key_value_count++;	
	
							//dim_values.add(Integer.toString(quarter_Fiscal));
							//dim_keys.add(_dataTypes.get(key_value_count).getName()+"_Quarter_Fiscal");
							curr.put(_dataTypes.get(key_value_count).getName(), null);
							key_value_count++;	
	
							//dim_values.add(Integer.toString(week_Fiscal));
							//dim_keys.add(_dataTypes.get(key_value_count).getName()+"_Week_Fiscal");
							curr.put(_dataTypes.get(key_value_count).getName(), null);
							key_value_count++;	
						}
//					}
				}else
				{	
					Date dt = null;
//					SimpleDateFormat sdt = _dataTypes.get(key_value_count).getCompiledDateFormat();
//					int fiscalMonthOffset = _dataTypes.get(key_value_count).getFiscalMonthOffset();
//					boolean isYearEndFiscalYear = _dataTypes.get(key_value_count).isYearEndFiscalYear;
//					int firstDayOfWeek = _dataTypes.get(key_value_count).getFirstDayOfWeek();

					cal.setFirstDayOfWeek(firstDayOfWeek);

					
					if(columnValue instanceof String)
					{
						dt = sdt.parse((String)columnValue);
					}else
					{
						dt = (Date)columnValue;
					}
				
	
					if(columnValue instanceof String)
					{
						dim_values.add((String) columnValue);
					}else
					{
						dim_values.add(sdt.format(dt));
					}
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), dt);
					key_value_count++;
					
				    cal.setTime(dt);

				    int day = cal.get(Calendar.DAY_OF_MONTH);
				    int month = cal.get(Calendar.MONTH);
				    int year = cal.get(Calendar.YEAR);
				    int quarter = FiscalDateUtil.getCalendarQuarter(month);
				    int week = FiscalDateUtil.getCalendarWeek(cal, firstDayOfWeek);
				    int hour = cal.get(Calendar.HOUR_OF_DAY);
				    int minute = cal.get(Calendar.MINUTE);
				    int second = cal.get(Calendar.SECOND);
				    long sec_epoch = dt.getTime()/(1000);
				    long day_epoch = dt.getTime()/(1000*60*60*24);
				    
				    
					measure_values.add(sec_epoch);
					curr.put(_dataTypes.get(key_value_count).getName(), sec_epoch);
					key_value_count++;
	
					measure_values.add(day_epoch);
					curr.put(_dataTypes.get(key_value_count).getName(), day_epoch);
					key_value_count++;
	
				    // format number as String
				    dim_values.add(df.format(day));
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), day);
					key_value_count++;
	
					dim_values.add(df.format(month+1));
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), (month+1));
					key_value_count++;
	
					dim_values.add(df4.format(year));
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), year);
//				    System.out.println(_dataTypes.get(key_value_count).getName() + ": "+columnValue + " - "+ year);
					key_value_count++;
	
					dim_values.add(Integer.toString(quarter));
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), quarter);
					key_value_count++;
	
					dim_values.add(df.format(week));
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					curr.put(_dataTypes.get(key_value_count).getName(), week);
					key_value_count++;
					
					//This check is temporary remove when schema supports these new data parts
//					if(DatasetUtilConstants.createNewDateParts)
//					{		
						dim_values.add(df.format(hour));
						dim_keys.add(_dataTypes.get(key_value_count).getName());
						curr.put(_dataTypes.get(key_value_count).getName(), hour);
						key_value_count++;
	
						dim_values.add(df.format(minute));
						dim_keys.add(_dataTypes.get(key_value_count).getName());
						curr.put(_dataTypes.get(key_value_count).getName(), minute);
						key_value_count++;
	
						dim_values.add(df.format(second));
						dim_keys.add(_dataTypes.get(key_value_count).getName());
						curr.put(_dataTypes.get(key_value_count).getName(), second);
						key_value_count++;
	
						if(fiscalMonthOffset>0)
						{
							int fiscal_month = FiscalDateUtil.getFiscalMonth(month, fiscalMonthOffset);
							int fiscal_year = FiscalDateUtil.getFiscalYear(year, month, fiscalMonthOffset, isYearEndFiscalYear);
						    int fiscal_quarter = FiscalDateUtil.getFiscalQuarter(month, fiscalMonthOffset);
							int fiscal_week = FiscalDateUtil.getFiscalWeek(cal, fiscalMonthOffset, firstDayOfWeek);
	
							dim_values.add(df.format(fiscal_month+1));
							dim_keys.add(_dataTypes.get(key_value_count).getName());
							curr.put(_dataTypes.get(key_value_count).getName(), (fiscal_month+1));
							key_value_count++;
			
							dim_values.add(df4.format(fiscal_year));
							dim_keys.add(_dataTypes.get(key_value_count).getName());
							curr.put(_dataTypes.get(key_value_count).getName(), fiscal_year);
							key_value_count++;
			
							dim_values.add(Integer.toString(fiscal_quarter));
							dim_keys.add(_dataTypes.get(key_value_count).getName());
							curr.put(_dataTypes.get(key_value_count).getName(), fiscal_quarter);
							key_value_count++;
							
							dim_values.add(df.format(fiscal_week));
							dim_keys.add(_dataTypes.get(key_value_count).getName());
							curr.put(_dataTypes.get(key_value_count).getName(), fiscal_week);
							key_value_count++;
						}
//					}
					
				}

			} catch(Throwable t)
			{
				throw new ParseException(_dataTypes.get(key_value_count).getName() +" is not in specified date format {"+ _dataTypes.get(key_value_count).getFormat() + "}: " + columnValue +" - " +t.getMessage(),0);
			}

			}else 
			{
				if(columnValue!=null)
				{					
					int precision = _dataTypes.get(key_value_count).getPrecision() > 0 ? _dataTypes.get(key_value_count).getPrecision(): maxTextLength;
					if(_dataTypes.get(key_value_count).isMultiValue())
					{
						if(_dataTypes.get(key_value_count).getMultiValueSeparator()==null)
							_dataTypes.get(key_value_count).setMultiValueSeparator(";"); //Default MultivalueSeparator
						String vals[] = columnValue.toString().split(Pattern.quote(_dataTypes.get(key_value_count).getMultiValueSeparator()));
						for(String val:vals)
						{
							if(val!=null)
							{
								if(val.length() > precision)
									dim_values.add(val.substring(0, precision));
								else
									dim_values.add(val);
								dim_keys.add(_dataTypes.get(key_value_count).getName());
							}
						}
					}else
					{
						if(columnValue.toString().length() > precision)
							dim_values.add(columnValue.toString().substring(0, precision));
						else
							dim_values.add(columnValue.toString());
						dim_keys.add(_dataTypes.get(key_value_count).getName());
					}
				}
				curr.put(_dataTypes.get(key_value_count).getName(), columnValue);
				key_value_count++;
			}
			count++;
		}
		if(dim_values.size()>0)
		{
			arr(measure_values);
			dict(dim_keys, dim_values);
			successRowCount++;
		}else
		{
			String message = "Row " + totalRowCount + " contains no Text Values. Atleast 1 column should have a non empty value or default value";
			throw new IOException(message);
		}
		prev = curr;
	}
	
	
	
	protected void initmeasures(LinkedList<FieldType> _dataTypes)  throws IOException 
	{
		LinkedList<Integer> measure_index = new LinkedList<Integer>();
		int cnt = 0;
		for (FieldType dataType: _dataTypes)
		{
			if (!dataType.isSkipped && dataType.getfType() == FieldType.MEASURE) 
			{
				measure_index.add(cnt);
			}
			cnt++;
		}
	
		vInt(measure_index.size());
		for (int i = 0; i < measure_index.size(); i++) {
			vInt(_dataTypes.get((int) measure_index.get(i)).getName().getBytes("UTF-8").length);
			out.write(_dataTypes.get((int) measure_index.get(i)).getName().getBytes("UTF-8"));
			vInt(2); //vInt(_dataTypes.get((int) measure_index.get(i)).getMeasure_type());			
			vInt(_dataTypes.get((int) measure_index.get(i)).getMeasure_multiplier());		
		}
		out.write(checksum);
	}

	protected void vInt(long i) throws IOException
	{
		byte b = 0;
		while ((i & ~0x7F) != 0) {
			b = (byte) ((i & 0x7F) | 0x80);
			out.write(b);
			i = i >> 7;
		}
		out.write((int)i);
	}

	protected void dict(LinkedList<String> dim_keys, LinkedList<String> dim_values)  throws IOException
	{
//		System.out.println(dim_keys);
//		System.out.println(dim_values);
		int dlen = dim_values.size();
		vInt(dlen);
		for (int i = 0; i < dlen; i++) {
            byte[] v = dim_values.get(i).getBytes("UTF-8");
            byte[] d = dim_keys.get(i).getBytes("UTF-8");
			vInt(v.length + d.length + 1);
			out.write(d);   	
			out.write(delimiter);
			out.write(v);
		}
		out.write(checksum);
	}

	/*
	protected void dict(LinkedList<String> dim_keys, LinkedList<String> dim_values)  throws IOException
	{
	       int dlen = dim_values.size();
	       vInt(dlen);
//	       delimiter = ":".getBytes("UTF-8");
	       for (int i = 0; i < dlen; i++) {
	              byte[] v = dim_values.get(i).getBytes("UTF-8");
	              byte[] d = dim_keys.get(i).getBytes("UTF-8");
	              vInt(v.length + d.length + 1);
	              byte[] output = new byte[v.length + d.length + delimiter.length];
	              System.arraycopy(d, 0, output, 0, d.length);
	              System.arraycopy(delimiter, 0, output, d.length, delimiter.length);
	              System.arraycopy(v, 0, output, d.length+delimiter.length, v.length);
	              out.write(output);        
	       }
	       out.write(checksum);
	}
	*/
	
	protected void arr(LinkedList<Long> measure_values)  throws IOException
	{
		for (int i = 0; i < measure_values.size(); i++) {
			vEncodedIntCustom(measure_values.get(i));    	
		}
	}
		
	public void finish() throws IOException
	{
		if (out != null) {
			out.flush();
			IOUtils.closeQuietly(out);
		}
		out = null;
		if(totalRowCount!=0)
		{
			long newStartTime = System.currentTimeMillis();
			if(startTime==0)
				startTime = newStartTime;
			logger.println("Processed last row {"+nf.format(totalRowCount) +"} time {"+nf.format(newStartTime-startTime)+"}");			
			startTime = newStartTime;
		}
	}
	
	private void vEncodedIntCustom(long i) throws IOException {
		long res = 0;
		if (i < 0) {
			res = (long) -i * 2 - 1;
		} else {
			res = (long) i * 2;
		}
		vInt(res);
	}
	
	public static boolean isValidBin(byte[] startingFiveBytes)
	{
		if(startingFiveBytes == null || startingFiveBytes.length < 5 )
			return false;
		for(int i=0;i<5;i++)
		{
		  if(i<3)
		  {
			  if(startingFiveBytes[i] != magic[i])
				  return false;
		  }else if(i==3)
		  {
			  if(startingFiveBytes[i] != version_high)
				  return false;
		  }else if(i==4)
		  {
			  if(startingFiveBytes[i] != version_low)
				  return false;
		  }
		}
		  return true;	
	}
	
	public int getSuccessRowCount() {
		return successRowCount;
	}


	public int getTotalRowCount() {
		return totalRowCount;
	}

	
    public int getFiscalWeek(int fiscalMonthOffset, int weeksinYear, int firstDayOfWeek, int year, int week) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.setFirstDayOfWeek(firstDayOfWeek);
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, fiscalMonthOffset);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        int weekOffset = calendar.get(Calendar.WEEK_OF_YEAR)-1;
        int result = ((week - weekOffset - 1) % weeksinYear) + 1;
        if (result <= 0) {
            result += weeksinYear;
        }
        return result;
    }

}
