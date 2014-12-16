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
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.util.DatasetUtils;

public class ExternalFileSchema {

	private static final String SCHEMA_FILE_SUFFIX = "_schema.json";

	public FileFormat fileFormat; 
	public LinkedList<ObjectType> objects;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((fileFormat == null) ? 0 : fileFormat.hashCode());
		result = prime * result + ((objects == null) ? 0 : objects.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ExternalFileSchema other = (ExternalFileSchema) obj;
		if (fileFormat == null) {
			if (other.fileFormat != null) {
				return false;
			}
		} else if (!fileFormat.equals(other.fileFormat)) {
			return false;
		}
		if (objects == null) {
			if (other.objects != null) {
				return false;
			}
		} else if (!objects.equals(other.objects)) {
			return false;
		}
		return true;
	}
	
	public static ExternalFileSchema init(File csvFile, Charset fileCharset, PrintStream logger) throws JsonParseException, JsonMappingException, IOException
	{
		ExternalFileSchema newSchema = null;
		//try 
		//{
			ExternalFileSchema userSchema = ExternalFileSchema.load(csvFile, fileCharset, logger);
			ExternalFileSchema autoSchema = ExternalFileSchema.createAutoSchema(csvFile, userSchema, fileCharset, logger);
			
			if(userSchema==null)
			{
				ExternalFileSchema.save(csvFile, autoSchema, logger);
				userSchema = autoSchema; 
			}

			if(userSchema!=null && !userSchema.equals(autoSchema))
			{			
				ExternalFileSchema schema = ExternalFileSchema.merge(userSchema, autoSchema, logger);
				if(!schema.equals(userSchema))
				{
					logger.println("Saving merged schema");
					ExternalFileSchema.save(csvFile, schema, logger);
				}
//				newSchema = ExternalFileSchema.load(csvFile);
				newSchema = schema;
			}else
			{
				newSchema = autoSchema;
			}
		//} catch (Throwable t) {
		//	t.printStackTrace();
		//}
		validateSchema(newSchema, logger);			
		return newSchema;
	}


	
	public static ExternalFileSchema createAutoSchema(File csvFile, ExternalFileSchema userSchema, Charset fileCharset, PrintStream logger) throws IOException
	{
		ExternalFileSchema emd = null;
		String baseName = FilenameUtils.getBaseName(csvFile.getName());
		baseName = createDevName(baseName, "Object", 0);
		String fullyQualifiedName = baseName; 
		//try 
		//{
			if(userSchema!=null)
			{
				if(userSchema.objects!=null && userSchema.objects.size()==1)
				{
					baseName = userSchema.objects.get(0).name;
					//because fully qualified name is used to match auto schema we will use user specified 
					fullyQualifiedName = userSchema.objects.get(0).name; 
				}
			}
		
			DetectFieldTypes detEFT = new DetectFieldTypes();
			LinkedList<FieldType> fields = detEFT.detect(csvFile, userSchema, fileCharset, logger);
			FileFormat fileFormat = new FileFormat();

			ObjectType od = new ObjectType();
			od.name = baseName;
			od.fullyQualifiedName = fullyQualifiedName;
			od.label = baseName;
			od.description = baseName;
			od.connector = "SalesforceAnalyticsCloudDatasetLoader";
//			od.isPrimaryObject = true;
//			od.rowLevelSecurityFilter = "";
			od.fields = fields;
			
			LinkedList<ObjectType> objects = new LinkedList<ObjectType>();
			objects.add(od);			
			
			emd = new ExternalFileSchema();
			emd.fileFormat = fileFormat;
			emd.objects = objects;

		//} catch (Throwable t) {
		//	t.printStackTrace();
		//}
		validateSchema(emd, logger);
		return emd;
	}

	
	public static void save(File schemaFile,ExternalFileSchema emd, PrintStream logger)
	{
		ObjectMapper mapper = new ObjectMapper();	
		try 
		{
			if(!schemaFile.getName().endsWith(SCHEMA_FILE_SUFFIX))
			{
				FilenameUtils.getBaseName(schemaFile.getName());
				schemaFile = new File(schemaFile.getParent(),FilenameUtils.getBaseName(schemaFile.getName())+SCHEMA_FILE_SUFFIX);
			}
			mapper.writerWithDefaultPrettyPrinter().writeValue(schemaFile, emd);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
	public static ExternalFileSchema load(File inputCSV, Charset fileCharset, PrintStream logger) throws JsonParseException, JsonMappingException, IOException
	{
		File schemaFile = inputCSV;
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		boolean isCSV = false;
		if(!inputCSV.getName().endsWith(SCHEMA_FILE_SUFFIX))
		{
			schemaFile = new File(inputCSV.getParent(),FilenameUtils.getBaseName(inputCSV.getName())+SCHEMA_FILE_SUFFIX);
		}
		ExternalFileSchema userSchema = null;
		if(schemaFile.exists())
		{
			logger.println("Loading existing schema from file {"+ schemaFile +"}");
			userSchema  =  mapper.readValue(schemaFile, ExternalFileSchema.class);			
		}

		if(userSchema==null)
			return null;
		
		if(FilenameUtils.getExtension(inputCSV.getName()).equalsIgnoreCase("csv"))
		{			
			isCSV = true;
		}

			
			String[] header = null;
			if(isCSV)
			{
				CsvListReader reader = null;
				try 
				{
//					reader = new CsvReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCSV), false), DatasetUtils.utf8Decoder(null, fileCharset)));
					reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCSV), false), DatasetUtils.utf8Decoder(null , fileCharset)), CsvPreference.STANDARD_PREFERENCE);
					header = reader.getHeader(true);
//					if(header!=null)
//					{
////					header = reader.getHeaders();		
////					String[] nextLine;
////					reader = new CSVReader(new FileReader(inputCSV));			
////					header = reader.readNext();
//					if(reader!=null)
//					{
//						try {
//							reader.close();
//							reader = null;
//						} catch (Throwable e) {
//						}
//					}
//					}
				}catch(Throwable t){t.printStackTrace();}
				finally{
					if(reader!=null)
					{
						try {
							reader.close();
							reader = null;
						} catch (Throwable e) {
						}
					}
				}
			}
			
			if(header != null && header.length > 0)
			{
					String devNames[] = ExternalFileSchema.createUniqueDevName(header);
					if(userSchema != null)
					{					
						LinkedList<ObjectType> obj = userSchema.objects;
						if(obj!= null && !obj.isEmpty())
						{
							List<FieldType> fields = obj.get(0).fields;
							if(fields!= null && !fields.isEmpty())
							{
								List<FieldType> fieldsCopy = new LinkedList<FieldType>(fields);
								for(FieldType field:fieldsCopy)
								{
									if(field.isComputedField)
										continue;
									boolean found = false;
									for (int i=0; i< devNames.length; i++) 
									{
										if(field.getName().equals(devNames[i]))
										{
											found = true;
											break;
										}
									}
									if(!found)
									{
										logger.println("Field {"+field.getName()+"} not found in schema file {"+ schemaFile +"}");
										fields.remove(field);
									}
										
								}
							}
						}
					}
			}
			validateSchema(userSchema, logger);
			return userSchema;
	}
	

	private static void validateSchema(ExternalFileSchema schema, PrintStream logger) throws IllegalArgumentException
	{
		StringBuffer message = new StringBuffer();
		if(schema!=null)
		{
			LinkedList<ObjectType> user_objects = schema.objects;
			if(user_objects!=null && !user_objects.isEmpty())
			{
				int objectCount=0;
				for(ObjectType user_object:user_objects)
				{
						objectCount++;
						List<FieldType> user_fields = user_object.fields;
						if(user_object.name==null||user_object.name.trim().isEmpty())
						{
							message.append("[objects["+objectCount+"].name] in schema cannot be null or empty\n");
						}

						if(user_object.label==null||user_object.label.trim().isEmpty())
						{
							message.append("[objects["+objectCount+"].label] in schema cannot be null or empty\n");
						}

						if(user_object.fullyQualifiedName==null||user_object.fullyQualifiedName.trim().isEmpty())
						{
							message.append("[objects["+objectCount+"].fullyQualifiedName] in schema cannot be null or empty\n");
						}
						
						if(!createDevName(user_object.name, "Dataset", (objectCount-1)).equals(user_object.name))
						{
							message.append("Object name {"+user_object.name+"} contains invalid characters \n");
						}

						if(user_fields!=null && !user_fields.isEmpty())
						{
							HashSet<String> field = new HashSet<String>();
							int fieldCount=0;
							for(FieldType user_field:user_fields)
							{
								fieldCount++;
								if(user_field != null && user_field.name!=null || !user_field.name.isEmpty())
								{
									if(user_field.name.length()>255)
									{
										message.append("field name {"+user_field.name+"} is greater than 255 characters\n");
									}

									if(!createDevName(user_field.name, "Column", (fieldCount-1)).equals(user_field.name))
									{
										message.append("field name {"+user_field.name+"} contains invalid characters \n");
									}
									
									if(!field.add(user_field.name))
									{
										message.append("Duplicate field name {"+user_field.name+"}\n");
									}
									
									if(user_field.name.endsWith("_sec_epoch") || user_field.name.endsWith("_day_epoch") || user_field.name.endsWith("_Day") || user_field.name.endsWith("_Month") || user_field.name.endsWith("_Year") || user_field.name.endsWith("_Quarter") || user_field.name.endsWith("_Week"))
									{
										for(FieldType user_field_2:user_fields)
										{
											if(user_field_2 !=null && user_field_2.type!=null && user_field_2.type.equalsIgnoreCase("date") && user_field.name.contains(user_field_2.name))
											{
												message.append("field name {"+user_field.name+"} not allowed. When there is field {"+user_field_2.name+"} of type Date\n");
											}
										}
									}
									
									if(user_field.label!=null || user_field.label.trim().isEmpty())
									{
										if(user_field.label.length()>255)
										{
											message.append("field label {"+user_field.label+"} is greater than 255 characters\n");
										}
									}else
									{
										message.append("[objects["+objectCount+"].fields["+fieldCount+"].label] in schema cannot be null or empty\n");
									}

									if(user_field.fullyQualifiedName!=null || user_field.fullyQualifiedName.trim().isEmpty())
									{
										if(user_field.fullyQualifiedName.length()>255)
										{
											message.append("field fullyQualifiedName {"+user_field.fullyQualifiedName+"} is greater than 255 characters\n");
										}
									}else
									{
										message.append("[objects["+objectCount+"].fields["+fieldCount+"].fullyQualifiedName] in schema cannot be null or empty\n");
									}
									
									if(user_field.getfType()==FieldType.MEASURE)
									{
										if(user_field.getDefaultValue()==null|| !isLatinNumber(user_field.getDefaultValue()))
										{
											message.append("field fullyQualifiedName {"+user_field.fullyQualifiedName+"}  in schema must have default numeric value\n");
										}
									}
									
								}else
								{
									message.append("[objects["+objectCount+"].fields["+fieldCount+"].name] in schema cannot be null or empty\n");
								}
							}
						}else
						{
							message.append("[objects["+objectCount+"].fields] field in schema cannot be null or empty\n");
						}
				}
			}else
			{
				message.append("[objects] field in schema cannot be null or empty\n");
			}
		}
		if(message.length()!=0)
		{
			throw new IllegalArgumentException(message.toString());
		}
	}

	public static ExternalFileSchema merge(ExternalFileSchema userSchema, ExternalFileSchema autoSchema, PrintStream logger)
	{
		ExternalFileSchema mergedSchema = null;
		try 
		{
			if(userSchema == null)
			{
				return autoSchema;
			}
			LinkedList<ObjectType> user_objects = userSchema.objects;
			LinkedList<ObjectType> auto_objects = autoSchema.objects;
			LinkedList<ObjectType> merged_objects = new LinkedList<ObjectType>();
			if(user_objects==null)
				user_objects = auto_objects;
			for(ObjectType auto_object:auto_objects)
			{
				ObjectType merged_object = null;
				for(ObjectType user_object:user_objects)
				{
					if(auto_object.fullyQualifiedName.equals(user_object.fullyQualifiedName))
					{
						merged_object = new ObjectType();
						List<FieldType> user_fields = user_object.fields;
						List<FieldType> auto_fields = auto_object.fields;
						LinkedList<FieldType> merged_fields = new LinkedList<FieldType>();
						if(user_fields==null || user_fields.isEmpty())
						{
							user_fields = auto_fields;
						}
						if(auto_fields==null || auto_fields.isEmpty())
						{
							auto_fields = user_fields;
						}
						for(FieldType auto_field:auto_fields)
						{
							FieldType merged_field = null;
							boolean found = false;
							for(FieldType user_field:user_fields)
							{
								if(auto_field.fullyQualifiedName.equals(user_field.fullyQualifiedName))
								{
									found = true;
									if(!auto_field.equals(user_field))
									{
										logger.println("Field {"+user_field+"} has been modified by user");
										merged_field = new FieldType(user_field.name!=null?user_field.name:auto_field.name);
										merged_field.type =  user_field.type!=null?user_field.type:auto_field.type;
//										merged_field.acl =  user_field.acl!=null?user_field.acl:auto_field.acl;
										merged_field.defaultValue =  user_field.defaultValue!=null?user_field.defaultValue:auto_field.defaultValue;
										merged_field.description =  user_field.description!=null?user_field.description:auto_field.description;
										merged_field.fiscalMonthOffset =  user_field.fiscalMonthOffset!=0?user_field.fiscalMonthOffset:auto_field.fiscalMonthOffset;
										merged_field.setFormat(user_field.format!=null?user_field.format:auto_field.format);
										merged_field.fullyQualifiedName =  user_field.fullyQualifiedName!=null?user_field.fullyQualifiedName:auto_field.fullyQualifiedName;
//										merged_field.isAclField =  user_field.isAclField!=false?user_field.isAclField:auto_field.isAclField;
										merged_field.isMultiValue =  user_field.isMultiValue!=false?user_field.isMultiValue:auto_field.isMultiValue;
//										merged_field.isNillable =  user_field.isNillable!=true?user_field.isNillable:auto_field.isNillable;
										merged_field.isSystemField =  user_field.isSystemField!=false?user_field.isSystemField:auto_field.isSystemField;
										merged_field.isUniqueId =  user_field.isUniqueId!=false?user_field.isUniqueId:auto_field.isUniqueId;
										merged_field.label =  user_field.label!=null?user_field.label:auto_field.label;
										merged_field.multiValueSeparator =  user_field.multiValueSeparator!=null?user_field.multiValueSeparator:auto_field.multiValueSeparator;
//										merged_field.name =  user_field.name!=null?user_field.name:auto_field.name;
										merged_field.precision =  user_field.precision!=0?user_field.precision:auto_field.precision;
										merged_field.scale =  user_field.scale!=0?user_field.scale:auto_field.scale;
									}
								}								
							}
							if(!found)
							{
								logger.println("Found new field {"+auto_field+"} in CSV");
							}
							if(merged_field==null)
							{
								merged_field = auto_field;
							}
							merged_fields.add(merged_field);
						}

						for(FieldType user_field:user_fields)
						{
							if(user_field.isComputedField)
								merged_fields.add(user_field);
						}
						
						merged_object.acl =  user_object.acl!=null?user_object.acl:auto_object.acl;
						merged_object.connector =  user_object.connector!=null?user_object.connector:auto_object.connector;
						merged_object.description =  user_object.description!=null?user_object.description:auto_object.description;
						merged_object.fullyQualifiedName =  user_object.fullyQualifiedName!=null?user_object.fullyQualifiedName:auto_object.fullyQualifiedName;
						merged_object.label =  user_object.label!=null?user_object.label:auto_object.label;
						merged_object.name =  user_object.name!=null?user_object.name:auto_object.name;
//						merged_object.recordTypeIdentifier =  user_object.recordTypeIdentifier!=null?user_object.recordTypeIdentifier:auto_object.recordTypeIdentifier;
						merged_object.rowLevelSecurityFilter =  user_object.rowLevelSecurityFilter!=null?user_object.rowLevelSecurityFilter:auto_object.rowLevelSecurityFilter;
//						merged_object.isPrimaryObject =  user_object.isPrimaryObject!=true?user_object.isPrimaryObject:auto_object.isPrimaryObject;
						merged_object.fields =  merged_fields;						
					}
				}
				if(merged_object==null)
				{
					merged_object = auto_object;

				}
				merged_objects.add(merged_object);
			}
			mergedSchema = new ExternalFileSchema();
			mergedSchema.fileFormat = userSchema.fileFormat!=null?userSchema.fileFormat:autoSchema.fileFormat;
			mergedSchema.objects = merged_objects;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return mergedSchema;
	}


	public static File getSchemaFile(File csvFile, PrintStream logger) {
		try 
		{
			//init(csvFile);
			if(!csvFile.getName().endsWith(SCHEMA_FILE_SUFFIX))
			{
				FilenameUtils.getBaseName(csvFile.getName());
				csvFile = new File(csvFile.getParent(),FilenameUtils.getBaseName(csvFile.getName())+SCHEMA_FILE_SUFFIX);
				return csvFile;
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}
	
	public static String[] createUniqueDevName(String[] headers) 
	{
		if(headers==null)
			return null;
		LinkedList<String> originalColumnNames = new LinkedList<String>();
		LinkedList<String> uniqueColumnNames = new LinkedList<String>();
		LinkedList<String> devNames = new LinkedList<String>();
		for(int i=0;i<headers.length;i++)
		{
			originalColumnNames.add(headers[i]);
			String devName = createDevName(headers[i], "Column", i);
				devNames.add(devName);
		}
			
		for(int i=0;i<headers.length;i++)
		{
			String newName = devNames.get(i);
			if(uniqueColumnNames.contains(newName))
			{
				int index = 1;
				while(true)
				{
					newName = devNames.get(i) + index;
					if(!uniqueColumnNames.contains(newName) && !originalColumnNames.subList(i+1, devNames.size()).contains(newName))
					{
						break;
					}
					index++;
				}
			}else
			{
				//Did we change the column name? if yes check if have a collision with existing columns
				if((headers[i] == null || !newName.equals(headers[i])) && originalColumnNames.subList(i+1, devNames.size()).contains(newName))
				{
					int index = 1;
					while(true)
					{
						newName = devNames.get(i) + index;
						if(!uniqueColumnNames.contains(newName) && !originalColumnNames.subList(i+1, devNames.size()).contains(newName))
						{
							break;
						}
						index++;
					}
				}
			}
			uniqueColumnNames.add(newName);
		}
		return uniqueColumnNames.toArray(new String[0]);
	}
	
	public static String createDevName(String inString, String defaultName, int columnIndex) {
		String outString = inString;
		String suffix = null;
		try 
		{
			if(inString != null && !inString.trim().isEmpty())
			{
				StringBuffer outStr = new StringBuffer(inString.length()+1);
				if(inString.endsWith("__c") && !inString.equals("__c"))
				{
					suffix = "__c";
					inString = inString.substring(0,inString.length()-3);
				}
				@SuppressWarnings("unused")
				int index = 0;	
				boolean hasFirstChar = false;
				boolean lastCharIsUnderscore = false;
				for(char ch:inString.toCharArray())
				{
//					if(Character.isLetterOrDigit((int)ch))
					if(isLatinLetter(ch) || isLatinNumber(ch))
					{
//						if(!hasFirstChar && Character.isDigit((int)ch))
						if(!hasFirstChar && isLatinNumber(ch))
						{
								outStr.append('X');
						}
						outStr.append(ch);
						hasFirstChar = true;
						lastCharIsUnderscore = false;
					}else if(hasFirstChar && !lastCharIsUnderscore)
					{
						outStr.append('_');
						lastCharIsUnderscore = true;
//						if(!(ch == '_'))
//						{
//							lastCharIsUnderscore = true;
//						}
					}
					index++;
				}
			    if (!hasFirstChar) {
			    	outString = defaultName  + (columnIndex+1);
			    } else 
			    {
					outString = outStr.toString();
			    	if(outString.length() > 255) {
			    		outString= outString.substring(0, 255);
			    	}
			    	while(outString.endsWith("_") && outString.length() > 0) {
			    		outString = outString.substring(0, outString.length()-1);
			    	}
			    	if(outString.isEmpty())
			    	{
				    	outString = defaultName  + (columnIndex+1);
			    	}else
			    	{
			    		if(suffix!=null)
			    			outString = outString + suffix;
			    	}
			    }
			}else
			{
				outString = defaultName  + (columnIndex+1);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return outString;
	}
	
	@SuppressWarnings("unused")
	private static String replaceString(String original, String pattern, String replace) 
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
	
	public static boolean isLatinLetter(char c) {
	    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
	}
	
	public static boolean isLatinNumber(char c) {
	    return (c >= '0' && c <= '9');
	}

	public static boolean isLatinNumber(String str) {
		if (str == null) {
			return false;
		}
		int sz = str.length();
		if (sz == 0) {
			return false;
		}

		for (int i = 0; i < sz; i++) {
			char c = str.charAt(i);
			if (!(c >= '0' && c <= '9'))
				return false;
		}
		return true;
	}

	
}
