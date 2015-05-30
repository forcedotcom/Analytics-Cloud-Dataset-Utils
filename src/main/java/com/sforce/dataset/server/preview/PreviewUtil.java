package com.sforce.dataset.server.preview;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.sforce.dataset.loader.DatasetLoaderException;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.loader.file.schema.ext.FieldType;
import com.sforce.dataset.loader.file.schema.ext.ObjectType;
import com.sforce.dataset.util.CharsetChecker;
import com.sforce.dataset.util.DatasetUtils;

public class PreviewUtil {
	
	public static List<Header> getFileHeader(File inputFile) throws JsonParseException, JsonMappingException, IOException, DatasetLoaderException
	{
			Charset tmp = null;
			try 
			{
					tmp = CharsetChecker.detectCharset(inputFile,System.out);
			} catch (Exception e) 
			{
			}

			if(tmp==null)
			{
				tmp = Charset.forName("UTF-8");
			}
			ExternalFileSchema schema = ExternalFileSchema.init(inputFile, null, System.out);
			List<Header> columns = convertSchemaToHeader(schema);
			if(columns==null || columns.isEmpty())
			{
				throw new IllegalArgumentException("Invalid schema file: " + ExternalFileSchema.getSchemaFile(inputFile, System.out));
			}			
			return columns;
	}
	
	public static List<Map<String, String>> getFileData(File inputFile) throws JsonParseException, JsonMappingException, IOException, DatasetLoaderException
	{
			Charset tmp = null;
			try 
			{
					tmp = CharsetChecker.detectCharset(inputFile,System.out);
			} catch (Exception e) 
			{
			}

			if(tmp==null)
			{
				tmp = Charset.forName("UTF-8");
			}

			ExternalFileSchema schema = ExternalFileSchema.init(inputFile, tmp, System.out);
			CsvPreference pref = new CsvPreference.Builder((char) CsvPreference.STANDARD_PREFERENCE.getQuoteChar(), schema.getFileFormat().getFieldsDelimitedBy().charAt(0), CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).build();
			CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputFile), false), DatasetUtils.utf8Decoder(CodingErrorAction.REPORT, tmp )), pref);				

			boolean hasmore = true;
			int totalRowCount = 0;
			@SuppressWarnings("unused")
			int errorRowCount = 0;
			List<Map<String,String>> data = new LinkedList<Map<String,String>>();
			while (hasmore) 
			{
				try
				{
					List<String> row = reader.read();
					if(row!=null)
					{
						totalRowCount++;
						if(totalRowCount==1)
							continue;
						if(row.size()!=0 )
						{
							Map<String,String> map = new HashMap<String,String>();
							int i=0;
							for(FieldType fld:schema.getObjects().get(0).getFields())
							{
								if(row.size()>i)
								{
									map.put(fld.getName(), row.get(i));
								}else
								{
									map.put(fld.getName(), null);
								}
								i++;
							}
							map.put("_id",(totalRowCount-1)+"");
							data.add(map);
						}else
						{
							errorRowCount++;
						}
						
					}else
					{
						hasmore = false;
					}
				}catch(Exception t)
				{
					errorRowCount++;
					Map<String,String> map = new HashMap<String,String>();
					int i=0;
					for(FieldType fld:schema.getObjects().get(0).getFields())
					{
						if(i==0)
						{
							map.put(fld.getName(), "Line {"+(totalRowCount)+"} has error {"+t+"}");
						}else
						{
							map.put(fld.getName(), null);
						}
						i++;
					}
					data.add(map);
				}
			}//end while
			if(reader!=null)
				IOUtils.closeQuietly(reader);
			return data;
	}

	
	private static List<Header> convertSchemaToHeader(ExternalFileSchema schema)
	{
		List<Header> columns = null;
		if(schema != null)
		{					
			LinkedList<ObjectType> obj = schema.getObjects();
			if(obj!= null && !obj.isEmpty())
			{
				List<FieldType> fields = obj.get(0).getFields();
				if(fields!= null && !fields.isEmpty())
				{
					columns = new LinkedList<Header>();
					for(FieldType field:fields)
					{
						if(field.isComputedField)
							continue;
						Header temp = new Header();
						temp.setField(field.getName());
						temp.setId(field.getName());
						temp.setName(field.getLabel());
						if(field.getfType()==FieldType.MEASURE)
							temp.setWidth(120);
						else if(field.getfType()==FieldType.DATE)
							temp.setWidth(160);
						else
							temp.setWidth(160);							
						columns.add(temp);
					}
				}
			}
		}
		return columns;
	}


}
