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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import com.sforce.dataset.loader.file.schema.FieldType;


public class EbinFormatWriter {
	
	static byte[] delimiter = new byte[] {(byte)0x3A}; // ":"
	static final byte[] magic = new byte[] {(byte)0x11, (byte)0xff, (byte)0x48};
	static final byte version_high = 1;
	static final byte version_low = 0;
	static final byte checksum = 17;
	static final long edgeMinValue = -36028797018963968L;
	static final long edgeMaxValue = 36028797018963967L;
	static final int maxTextLength = 256;

	
	LinkedList<Integer> measure_index = new  LinkedList<Integer>();
	LinkedList<FieldType> _dataTypes = new  LinkedList<FieldType>();

	private int numColumns = 0;	
	private OutputStream out;
	protected long nrows;
	int interval = 10;

	public static final NumberFormat nf = NumberFormat.getIntegerInstance();
	long startTime = 0L;
	
	public EbinFormatWriter(OutputStream out, List<FieldType> dataTypes)
			throws IOException 
	{
		this(out,dataTypes.toArray(new FieldType[0]));
	}

	
	public EbinFormatWriter(OutputStream out, FieldType[] dataTypes)
			throws IOException 
	{
		this(out);
		this.numColumns = dataTypes.length;
		for (FieldType dataType: dataTypes)
		{
			_dataTypes.add(dataType);
			if (dataType.getfType() == FieldType.DATE)
			{
				_dataTypes.add(FieldType.GetMeasureKeyDataType(dataType.getName() + "_sec_epoch", 0 , 0, 0L));
				_dataTypes.add(FieldType.GetMeasureKeyDataType(dataType.getName() + "_day_epoch", 0 , 0, 0L));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Day", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Month", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Year", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Quarter", null, null));
				_dataTypes.add(FieldType.GetStringKeyDataType(dataType.getName() + "_Week", null, null));
			}
		}

		this.initmeasures(_dataTypes);
	}
	
	protected EbinFormatWriter(OutputStream out)  throws IOException
	{
		this.out = out;
		nrows = 0;
		out.write(magic, 0, 3);
		out.write(version_high);
		out.write(version_low);
//		out.writeByte(version_high);
//		out.writeByte(version_low);
 	}


	public void addrow(String[] values)  throws IOException,NumberFormatException, ParseException
	{
		LinkedList<Long> measure_values = new LinkedList<Long>();
		LinkedList<String> dim_keys = new LinkedList<String>();
		LinkedList<String> dim_values = new LinkedList<String>();
		
		int count = 0;
		int key_value_count = 0;

		nrows++;
		if (values.length != this.numColumns) {
			String message = "Row " + nrows + " contains an invalid number of Values, expected " + 
					this.numColumns + " Value(s), got " + values.length + ".\n";
			throw new IOException(message);
		}
				
		if(nrows%interval==0||nrows==1)
		{
			long newStartTime = System.currentTimeMillis();
			if(startTime==0)
				startTime = newStartTime;
			System.out.println("Processing row {"+nf.format(nrows) +"} time {"+nf.format(newStartTime-startTime)+"}");			
			startTime = newStartTime;
		}
		
		if(nrows/interval>=10)
		{
			interval = interval*10;
		}
		
		while(key_value_count<_dataTypes.size())
		{
			if(values[count]==null||values[count].trim().length()==0)
				values[count] = _dataTypes.get(key_value_count).getDefaultValue();

			if (_dataTypes.get(key_value_count).getfType() == FieldType.MEASURE) 
			{
				try
				{
					double v = 0L;
					if(values[count]==null||values[count].trim().length()==0)
					{
						measure_values.add((long)v);
						key_value_count++;
					}else
					{
						v = Double.parseDouble(values[count]);				
						if (_dataTypes.get(key_value_count).getMeasure_multiplier() > 1)
						{
							v = v*_dataTypes.get(key_value_count).getMeasure_multiplier();
						}
						if(v>edgeMaxValue || v<edgeMinValue)
							throw new ParseException("Value out of range ("+edgeMinValue+"-"+edgeMaxValue+")",0);
						measure_values.add((long)v);
						key_value_count++;
					}
				} catch(Throwable t)
				{
					throw new NumberFormatException(_dataTypes.get(key_value_count).getName() +" is not a valid number: " + values[count] +" - " +t.getMessage());
				}
			}else if (_dataTypes.get(key_value_count).getfType() == FieldType.DATE) 
			{
				try
				{
				
				if(values[count]==null||values[count].trim().length()==0)
				{
					//The date is null or blank so add default
					dim_values.add("");
					dim_keys.add(_dataTypes.get(key_value_count).getName());
					key_value_count++;

					measure_values.add(0L);
					key_value_count++;
					measure_values.add(0L);
					key_value_count++;

					//Skip the rest
				    //DecimalFormat df = new DecimalFormat("00");
				    //dim_values.add(df.parse(Integer.toString(day)).toString());
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					key_value_count++;

					//dim_values.add(df.parse(Integer.toString(month)).toString());
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					key_value_count++;

					//dim_values.add(Integer.toString(year));
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					key_value_count++;

					//dim_values.add(Integer.toString(quarter));
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					key_value_count++;

					//dim_values.add(Integer.toString(week));
					//dim_keys.add(_dataTypes.get(key_value_count).getName());
					key_value_count++;					
				}else
				{
				
				SimpleDateFormat sdt = new SimpleDateFormat(_dataTypes.get(key_value_count).getFormat());
				Date dt = sdt.parse(values[count]);
				Calendar cal = Calendar.getInstance();
			    cal.setTime(dt);

				dim_values.add(values[count]);
				dim_keys.add(_dataTypes.get(key_value_count).getName());
				key_value_count++;
				

			    int day = cal.get(Calendar.DAY_OF_MONTH);
			    int month = cal.get(Calendar.MONTH)+1;
			    int year = cal.get(Calendar.YEAR);
			    //int quarter = (int)((month-1)/3 + 1); 
			    int quarter = (int) ((((Math.ceil(1.0*(22-_dataTypes.get(key_value_count).getFiscalMonthOffset()+month-1)/3.0)*3.0)/3)%4)+1);
			    int week = cal.get(Calendar.WEEK_OF_YEAR);
			    long sec_epoch = dt.getTime()/(1000);
			    long day_epoch = dt.getTime()/(1000*60*60*24);

				measure_values.add(sec_epoch);
				key_value_count++;
				measure_values.add(day_epoch);
				key_value_count++;

			    // format number as String
			    DecimalFormat df = new DecimalFormat("00");
			    df.setMinimumIntegerDigits(2);
			    dim_values.add(df.format(day));
				dim_keys.add(_dataTypes.get(key_value_count).getName());
				key_value_count++;

				dim_values.add(df.format(month));
				dim_keys.add(_dataTypes.get(key_value_count).getName());
				key_value_count++;

				dim_values.add(Integer.toString(year));
				dim_keys.add(_dataTypes.get(key_value_count).getName());
				key_value_count++;

				dim_values.add(Integer.toString(quarter));
				dim_keys.add(_dataTypes.get(key_value_count).getName());
				key_value_count++;

				dim_values.add(df.format(week));
				dim_keys.add(_dataTypes.get(key_value_count).getName());
				key_value_count++;
				}

			} catch(Throwable t)
			{
				throw new ParseException(_dataTypes.get(key_value_count).getName() +" is not in specified date format {"+ _dataTypes.get(key_value_count).getFormat() + "}: " + values[count] +" - " +t.getMessage(),0);
			}

			}
			else {
				if(_dataTypes.get(key_value_count).isMultiValue())
				{
					String vals[] = values[count].split(Pattern.quote(_dataTypes.get(key_value_count).getMultiValueSeparator()));
					for(String val:vals)
					{
						if(val!=null && val.length() > maxTextLength)
							dim_values.add(val.substring(0, maxTextLength));
						else
							dim_values.add(val);
						dim_keys.add(_dataTypes.get(key_value_count).getName());
					}
				}else
				{
					if(values[count]!=null && values[count].length() > maxTextLength)
						dim_values.add(values[count].substring(0, maxTextLength));
					else
						dim_values.add(values[count]);
					dim_keys.add(_dataTypes.get(key_value_count).getName());
				}
				key_value_count++;
			}
			count++;
		}
		arr(measure_values);
		dict(dim_keys, dim_values);
	}
	
	
	
	protected void initmeasures(LinkedList<FieldType> _dataTypes)  throws IOException 
	{
		LinkedList<Integer> measure_index = new LinkedList<Integer>();
		int cnt = 0;
		for (FieldType dataType: _dataTypes)
		{
			if (dataType.getfType() == FieldType.MEASURE) 
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
//		out.writeByte(checksum);
	}

	protected void vInt(long i) throws IOException
	{
		byte b = 0;
		while ((i & ~0x7F) != 0) {
			b = (byte) ((i & 0x7F) | 0x80);
			out.write(b);
//			out.writeByte(b);
			i = i >> 7;
		}
		out.write((int)i);
//		out.writeByte((int)i);
	}

	/*
	protected void dict(LinkedList<String> dim_keys, LinkedList<String> dim_values)  throws IOException
	{
		int dlen = dim_values.size();
		vInt(dlen);
		for (int i = 0; i < dlen; i++) {
			String d = dim_keys.get(i);
			String v = dim_values.get(i);
//			System.err.println(d+": "+v);
			vInt(v.getBytes("UTF-8").length + d.getBytes("UTF-8").length + 1);
//			byte[] b1 = d.getBytes("UTF-8");
			out.write(d.getBytes("UTF-8"));   	
			out.write(delimiter);
//			byte[] b2 = v.getBytes("UTF-8");
			out.write(v.getBytes("UTF-8"));
		}
		out.write(checksum);
//		out.writeByte(checksum);
	}
	*/
	
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
	
	protected void arr(LinkedList<Long> measure_values)  throws IOException
	{
		for (int i = 0; i < measure_values.size(); i++) {
			vEncodedIntCustom(measure_values.get(i));    	
		}
	}
	
	/*
	public void addrow(String[] keys, String[] values, long[] measure_values)  throws IOException
	{
		nrows++;
		if (measure_values.length != measures.length) {
			String message = "Row " + nrows + " contains an invalid number of measures, expected " + 
					measures.length + " measure(s), got " + measure_values.length + ".\n";
			throw new IOException(message);
		}
		arr(measure_values);
		dict(keys, values);
	}
	*/
	
	public void finish() throws IOException
	{
		if (out != null) {
			out.flush();
			out.close();
		}
		out = null;
		if(nrows==0)
		{
			throw new IOException("Atleast one row must be written");
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
	
	public long getNrows() {
		return nrows;
	}

		
}
