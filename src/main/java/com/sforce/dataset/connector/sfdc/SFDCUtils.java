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
package com.sforce.dataset.connector.sfdc;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sforce.dataset.connector.ConnectorUtils;
import com.sforce.dataset.connector.InputPipeline;
import com.sforce.dataset.connector.OutputPipeline;
import com.sforce.dataset.connector.WriteOperation;
import com.sforce.dataset.connector.exception.ConnectionException;
import com.sforce.dataset.connector.exception.DataConversionException;
import com.sforce.dataset.connector.exception.DataReadException;
import com.sforce.dataset.connector.exception.DataWriteException;
import com.sforce.dataset.connector.exception.FatalException;
import com.sforce.dataset.connector.exception.MetadataException;
import com.sforce.dataset.connector.metadata.ObjectType;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;

public class SFDCUtils {

	private static final Log logger = LogFactory.getLog(SFDCUtils.class);
	
	private static final int MAX_BASE64_LENGTH = 7 * 1024 * 1024; 
	private static final int MAX_DECIMAL_PRECISION = 38;
	//private static final int TARGET_BATCH_SIZE = 200;

	//The $LastRunTime Parameter is in this format (including Single quotes) ['2013-04-24 18:24:56']
	private static final Pattern $lastRunTimePattern = Pattern.compile("('\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d')");

	//The $LastRunTime Parameter is in this format (including Single quotes) ['2013-04-24']
//	private static final Pattern $lastRunDatePattern = Pattern.compile("('\\d\\d\\d\\d-\\d\\d-\\d\\d')");

	//Timestamp columns values are passed the filter in this format (no single quotes) [2013-04-24 18:24:56]
//	private static final Pattern timeStampPattern = Pattern.compile("(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d)(\\.\\d+)?");

	//Date column values are also in the Timestamp format but the time is 00:00:00
	//private static final Pattern datePattern = Pattern.compile("(\\d\\d\\d\\d-\\d\\d-\\d\\d 00:00:00)");

	private static final  SimpleDateFormat lastRunTimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static
	{
		lastRunTimeDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	private static final  SimpleDateFormat lastRunTimeDateWithMilliSecondsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	static
	{
		lastRunTimeDateWithMilliSecondsFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	private static final  SimpleDateFormat sfdcDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	static
	{
		sfdcDateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	private static final  SimpleDateFormat lastRunDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	static
	{
		lastRunDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	private static final  SimpleDateFormat sfdcDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	static
	{
		sfdcDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	//These are operators used by filter	
//	private static String EQUALS_OPERATOR = "{0} = {1}";
//	private static String NOT_EQUALS_OPERATOR = "{0} != {1}";
//	private static String LESS_THAN_OPERATOR = "{0} < {1}";
//	private static String GREATER_THAN_OPERATOR = "{0} > {1}";
//	private static String LESS_THAN_OR_EQUALS_OPERATOR = "{0} <= {1}";
//	private static String GREATER_THAN_OR_EQUALS_OPERATOR = "{0} >= {1}";
//	private static String IS_NULL_OPERATOR = "{0} = {1}";
//	private static String IS_NOT_NULL_OPERATOR = "{0} != {1}";
//	private static final String CONTAINS_OPERATOR = "{0} LIKE {1}";	//User will add the %
//	private static final String STARTS_WITH_OPERATOR = "{0} LIKE {1}"; //User will add the %
//	private static final String ENDS_WITH_OPERATOR = "{0} LIKE {1}"; //User will add the %
	
	/*
	private static final Map<String,String> filterOperatorMap = new HashMap<String,String>();
	static {
		filterOperatorMap.put(FilterOperation.contains.name(), CONTAINS_OPERATOR);
		filterOperatorMap.put(FilterOperation.endsWith.name(), ENDS_WITH_OPERATOR);
		filterOperatorMap.put(FilterOperation.equals.name(), EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.greaterOrEquals.name(), GREATER_THAN_OR_EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.greaterThan.name(), GREATER_THAN_OPERATOR);
		filterOperatorMap.put(FilterOperation.isNotNull.name(), IS_NOT_NULL_OPERATOR);
		filterOperatorMap.put(FilterOperation.isNull.name(), IS_NULL_OPERATOR);
		filterOperatorMap.put(FilterOperation.lessOrEquals.name(), LESS_THAN_OR_EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.lessThan.name(), LESS_THAN_OPERATOR);
		filterOperatorMap.put(FilterOperation.notEquals.name(), NOT_EQUALS_OPERATOR);
		filterOperatorMap.put(FilterOperation.startsWith.name(), STARTS_WITH_OPERATOR);		
	};	
	*/
	
	//SFDC Object that are not supported
	private static List<String> excludedObjects = Arrays.asList(new String[]{"UserRecordAccess","Vote"});

		

	public static List<ObjectType> getObjectList(PartnerConnection partnerConnection, Pattern pattern, boolean isWrite)
			throws MetadataException 
	{
		logger.debug("SFDCUtils.getSFDCObjectList(" + pattern + ")");

		if (pattern == null || pattern.pattern().isEmpty())
			pattern = Pattern.compile(".*");

		List<ObjectType> backendObjectInfoList = null;

		try {
			// Make the describeGlobal() call
			DescribeGlobalResult describeGlobalResult = partnerConnection.describeGlobal();

			// Get the sObjects from the describe global result
			DescribeGlobalSObjectResult[] sobjectResults = describeGlobalResult.getSobjects();
			
			backendObjectInfoList = new ArrayList<ObjectType>(sobjectResults.length);

			// Write the name of each sObject to the console
			for (DescribeGlobalSObjectResult sObjectResult : sobjectResults) {	

				logger.trace(sObjectResult.getName()+": "+sObjectResult.isCreateable()+","+sObjectResult.isUpdateable()+","+sObjectResult.isDeletable());

				// Skip Objects that are deprecated
				if (sObjectResult.isDeprecatedAndHidden())
					continue;

				if(excludedObjects.contains(sObjectResult.getName()))
				{
					continue;
				}

				if(!pattern.matcher(sObjectResult.getName()).matches())
				{
					continue;
				}

				if(!sObjectResult.getQueryable())
				{
					continue;
				}

				// At this point all we know is that this is target
				// We dont know if this is write/update/delete/upsert
				// So we are going to exclude any object that is not writable
				if(isWrite){
					if((!sObjectResult.isUpdateable() || !sObjectResult.isCreateable() || !sObjectResult.isDeletable()))
					{
						continue;
					}
				}
			
				ObjectType obj = new ObjectType();
				obj.setConnector("sfdc");
				obj.setName(sObjectResult.getName());
				obj.setFullyQualifiedName(sObjectResult.getName());
				obj.setDescription(sObjectResult.getLabel());
				obj.setLabel(sObjectResult.getLabel());

				//Add it to the list
				backendObjectInfoList.add(obj);
			}			
		} catch (Throwable t) {
			t.printStackTrace();
			throw new MetadataException(t.toString());
		}

		return backendObjectInfoList;

	}

	
	public static List<ObjectType> getRelatedObjectList(
			PartnerConnection partnerConnection, ObjectType primaryRecordInfo, boolean isWrite) throws MetadataException {

		logger.debug("SFDCUtils.getRelatedObjectList(\""+ primaryRecordInfo.getFullyQualifiedName() + "\")");

		// These debug statements should help you understand what is being
		// passed back to your calls. You can comment these out if you like
		logger.info("***");
		logger.info("primaryRecordInfo.getRecordName: "+primaryRecordInfo.getName());
		logger.info("primaryRecordInfo.getCatalogName: "+primaryRecordInfo.getFullyQualifiedName());
		logger.info("primaryRecordInfo.getLabel: "+primaryRecordInfo.getLabel());
		logger.info("---");

		
		List<ObjectType> backendObjectInfoList = new ArrayList<ObjectType>();
		
		// Salesforce related objects can be many levels deep for Example
		// 'Contact.Account.Owner'
		// The Toolkit does Not have a good way to represent these hierarchies
		// in the object list which is essential a flat list.
		// What we need is Tree instead of a List
		// we are attempting to represent the object hierarchy tree as a string
		// and returning it in the Catalog name field

		//SFDC Connector does not support writing to multiple targets (AS YET)
		if(isWrite)
			return backendObjectInfoList;

		try 
		{
			// We need the SObject Type to use in describeSObjects() call. We
			// know we can get it from getRecordType() because we set it
			// in method getObjectList() above
			String sObjectType = primaryRecordInfo.getName();
			
			DescribeSObjectResult dsr = partnerConnection.describeSObject(sObjectType);
			// Now, retrieve metadata for each field
			for (int i = 0; i < dsr.getFields().length; i++) 
			{
				// Get the field
				com.sforce.soap.partner.Field field = dsr.getFields()[i];

				// Skip fields that are deprecated
				if (field.isDeprecatedAndHidden())
					continue;
				
				//Get the parents fully qualified name for example Contact, or Contact.Account
				String parentFullyQualifiedName = primaryRecordInfo.getFullyQualifiedName();
			
				//We are only interested in fields that are Relationships (Foreign Keys)
				if(field.getRelationshipName()!=null && !field.getRelationshipName().isEmpty() && field.getReferenceTo() != null && field.getReferenceTo().length!=0)
				{
					logger.info(field.getRelationshipName());
					logger.info(field.getRelationshipOrder());
					logger.info(ArrayUtils.toString(field.getReferenceTo()));

					for (String relatedSObjectType : field.getReferenceTo()) 
					{
						// This is what shows in the related object list object
						// list drop down when user clicks on get Siblings
						String objectName = field.getRelationshipName();

						// This will be used by this code to build SFDC SOQL
						// SFDC functionality. It is not used by Informatica.
						// I need it is because when we are querying
						// multiple objects we need to know the fully
						// qualified path of the object for example
						// if the parent is Account and the relationship name is Owner
						// Then fully qualified name will be Account.Owner 
						String fullyQualifiedObjectName = parentFullyQualifiedName + "." + objectName;
						
						// The Object Label, SFDC has polymorphic relationships
						// for example Account.Owner Can be related to User or
						// Group. We need to give users a way to choose the
						// correct Relationship. That is why we are showing the
						// Type with relationship name in the label
						// unfortunately not fully exposed in the Cloud UI
						String objectLabel = objectName + "(" + relatedSObjectType + ")";

						// BackendObjectInfo constructor takes three parameters:
						// BackendObjectInfo(String objectCanonicalName, String
						// objectName, String objectLabel)
						//
						// objectCanonicalName :
						// The fully qualified name of the object cannot be null
						// set it
						// same as objectName if you do not have a canonical
						// name
						// This value be returned in RecordInfo.getCatalogName()
						// objectName :
						// The object name string, cannot contain any spaces or
						// special characters, cannot be null. This is what
						// shows up in
						// the source/target object list drop down. This value
						// will be
						// returned in RecordInfo.getRecordName()
						// objectLabel :
						// This is the business name or label of the object (may
						// contain
						// spaces). This is only used in the UI (Future use)
						ObjectType boi = new ObjectType();
						boi.setFullyQualifiedName(fullyQualifiedObjectName);
						boi.setName(objectName);
						boi.setLabel(objectLabel);

						// Add it to the list
						backendObjectInfoList.add(boi);
					}
				}
			}
		} catch (com.sforce.ws.ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new MetadataException(e.toString());
		}
					
		return backendObjectInfoList;
	}



	public static List<com.sforce.dataset.connector.metadata.FieldType> getFieldList(ObjectType recordInfo,PartnerConnection partnerConnection, boolean isSource)
			throws MetadataException 
	{
		try 
		{
			logger.debug("SFDCUtils.getFieldList(\""+ recordInfo.getName() + "\")");

			List<com.sforce.dataset.connector.metadata.FieldType> fieldList = new ArrayList<com.sforce.dataset.connector.metadata.FieldType>();
			
			logger.info("***");
			logger.info("getRecordName: "+recordInfo.getName());
			logger.info("getFullyQualifiedName: "+recordInfo.getFullyQualifiedName());
			logger.info("getLabel: "+recordInfo.getLabel());
			logger.info("getDescription: "+recordInfo.getDescription());
			logger.info("---");
						
			DescribeSObjectResult dsr = partnerConnection.describeSObject(recordInfo.getName());			
			if (dsr != null) 
			{
				logger.debug("\n\n** Object Name: " + dsr.getName());
				         			         
				// Now, retrieve metadata for each field
				for (int i = 0; i < dsr.getFields().length; i++) 
				{
					// Get the field
					com.sforce.soap.partner.Field field = dsr.getFields()[i];

					// Skip fields that are deprecated
					if (field.isDeprecatedAndHidden())
						continue;

					// This is specific to SFDC
					// The location field itself does not have data, so skip it
					if (field.getType() != null
							&& FieldType.location.equals(field.getType()))
						continue;

					// Get the Java class corresponding to the SFDC FieldType
					Class<?> clazz = SFDCConnectorConstants.getJavaClassFromFieldType(field.getType());			        	 			        	 	            
					int precision = getPrecision(field, clazz);
					int scale = getScale(field, clazz);

					com.sforce.dataset.connector.metadata.FieldType bField = new com.sforce.dataset.connector.metadata.FieldType();
					bField.setType(clazz);
					bField.setPrecision(precision);
					bField.setScale(scale);
					bField.setLabel(field.getLabel());
					bField.setDescription(field.getInlineHelpText());
					bField.setFullyQualifiedName(recordInfo.getName()+"."+field.getName());
					if (field.getType().equals(FieldType.id)
							|| field.isExternalId())
						bField.setUniqueId(true);
					bField.setNillable(field.getNillable());
					bField.setFilterable(field.getFilterable());

					HashMap<String, String> customAttributes = new HashMap<String, String>();
			    		
		    		customAttributes.put("FieldType", field.getType().toString());
		    		customAttributes.put("isAutoNumber", field.isAutoNumber()+"");
		    		customAttributes.put("isCreateable", field.isCreateable()+"");
		    		customAttributes.put("isUpdateable", field.isUpdateable()+"");
		    		customAttributes.put("isExternalId", field.isExternalId()+"");
		    		customAttributes.put("isIdLookup", field.isIdLookup()+"");
		    		customAttributes.put("isCalculated", field.isCalculated()+"");
		    		customAttributes.put("getReferenceTo", ArrayUtils.toString(field.getReferenceTo()));			    		

		    		bField.setExtension(customAttributes);

		    		fieldList.add(bField);
				}
			}
			return fieldList;
		} catch (Throwable t) {
			t.printStackTrace();
			if (t instanceof MetadataException)
				throw (MetadataException) t;
			else
				throw new MetadataException(t.toString());
		}
	}	   
	   


	public static boolean read(PartnerConnection partnerConnection,
			OutputPipeline buffer, ObjectType object,List<com.sforce.dataset.connector.metadata.FieldType> fields, int batchSize, String filter) throws ConnectionException,
			DataReadException,
			DataConversionException, FatalException {
		try 
		{
			// These debug statements should help you understand what is being
			// passed back to your calls. You can comment these out if you like
			
			logger.info("***");
			logger.info("getRecordName: "+object.getName());
			logger.info("getCatalogName: "+object.getFullyQualifiedName());
			logger.info("getLabel: "+object.getLabel());
			logger.info("---");

			if(batchSize>0)
			{
				partnerConnection.setQueryOptions(batchSize);
			}else
			{
				partnerConnection.setQueryOptions(2000);
			}

			//Generate the SOQL using the FieldList and RecordInfo
			String soqlQuery = generateSOQL(object, fields, batchSize, filter);
			logger.info("SOQL: "+soqlQuery);
			
			//Query SFDC
			QueryResult qr = partnerConnection.query(soqlQuery);

			int rowsSoFar = 0;
			boolean done = false;
			if (qr.getSize() > 0) 
			{
				while (!done) {
					SObject[] records = qr.getRecords();
					for (int i = 0; i < records.length; ++i) {
						List<Object> rowData = new ArrayList<Object>(fields.size());
						for (int var = 0; var < fields.size(); var++) {
							String fieldName = fields.get(var).getFullyQualifiedName();
							Object value = getFieldValueFromQueryResult(fieldName,records[i]);
							if (value != null) {
								value = ConnectorUtils.toJavaDataType(value, fields.get(var).getClass().getCanonicalName());
							}
							rowData.set(var, value);
						}
						buffer.setData(rowData);
						rowsSoFar++;
						// If preview, exit the while loop after pagesize is reached
						if (batchSize>0 && i >= batchSize - 1)
							break;
					}

					// If its preview exit when the first set is done even if
					// pageSize is not reached
					if (qr.isDone() || batchSize>0) {
						done = true;
					} else {
						qr = partnerConnection.queryMore(qr.getQueryLocator());
					}
				}// End While
			}
    			logger.info("Query returned {" + rowsSoFar + "} rows");				
			return true;
		} catch (Throwable t) {
			t.printStackTrace();
			throw new FatalException(t.toString());
		}
	}	
	
    

	public static void write(PartnerConnection partnerConnection,InputPipeline inputPipeline,
			List<com.sforce.dataset.connector.metadata.FieldType> fieldList, ObjectType recordInfo,
			WriteOperation writeOperation) throws DataWriteException,
			FatalException 
	{
		logger.debug("SFDCUtils.write("+recordInfo.getName()+")");				
		try
		{

			// These debug statements should help you understand what is being
			// passed back to your calls. You can comment these out if you like
			logger.info("***");
			logger.info("getRecordName: "+recordInfo.getFullyQualifiedName());
			logger.info("getCatalogName: "+recordInfo.getName());
			logger.info("getLabel: "+recordInfo.getLabel());
			logger.info("---");
			
			String IDFieldName = null;
			String externalIDFieldName = null;
			boolean hasIDMapped = false;
			int IDFieldIndex = -1;

			boolean[] isAutoNumber = new boolean[fieldList.size()];
			boolean[] isCreateable = new boolean[fieldList.size()];
			boolean[] isUpdateable = new boolean[fieldList.size()];
			boolean[] isExternalId = new boolean[fieldList.size()];
			boolean[] isIdLookup = new boolean[fieldList.size()];
			boolean[] isCalculated = new boolean[fieldList.size()];
			String[] getReferenceTo = new String[fieldList.size()];
			String[] fieldType = new String[fieldList.size()];
			int j=0;
	    	for (com.sforce.dataset.connector.metadata.FieldType field : fieldList)
	    	{
					HashMap<String, String> customAtributes = field.getExtension();    			
		    		for (String customAtribute : customAtributes.keySet()) 
		    		{
		    			try 
		    			{
		    					if (customAtribute.equals("isAutoNumber")) {
		    						isAutoNumber[j] = Boolean.parseBoolean(customAtributes.get("isAutoNumber"));
		    					} else
		    					if (customAtribute.equals("isCreateable")) {
		    						isCreateable[j] = Boolean.parseBoolean(customAtributes.get("isCreateable"));
		    					} else
		    					if (customAtribute.equals("isUpdateable")) {
		    						isUpdateable[j] = Boolean.parseBoolean(customAtributes.get("isUpdateable"));
		    					} else
		    					if (customAtribute.equals("isExternalId")) {
		    						isExternalId[j] = Boolean.parseBoolean(customAtributes.get("isExternalId"));
		    						if(isExternalId[j])
		    	    					externalIDFieldName = field.getName();	    	    				
		    					} else
		    					if (customAtribute.equals("isIdLookup")) {
		    						isIdLookup[j] = Boolean.parseBoolean(customAtributes.get("isIdLookup"));
		    					} else		    						
		    					if (customAtribute.equals("isCalculated")) {
		    						isCalculated[j] = Boolean.parseBoolean(customAtributes.get("isCalculated"));
		    					} else 
		    					if (customAtribute.equals("getReferenceTo")) {
		    						getReferenceTo[j] = customAtributes.get("getReferenceTo");
		    						if(getReferenceTo[j]==null || getReferenceTo[j].isEmpty())
		    							getReferenceTo[j]=null;
		    					} else 
		    					if (customAtribute.equals("FieldType")) {
		    						fieldType[j] = customAtributes.get("FieldType");
		    						if(fieldType[j]==null || fieldType[j].isEmpty())
		    						{
		    							fieldType[j]=null;
		    						}else
		    						{
		    							if(fieldType[j].equals(com.sforce.soap.partner.FieldType.id.toString()))
		    							{
			    	    					hasIDMapped = true;
			    							IDFieldName = field.getName();
			    							IDFieldIndex = j;
		    							}
		    						}
		    					}			    							    					
		    			} catch (Throwable t) {
		    				t.printStackTrace();
		    			}
		    		}		    		    		
		    				    		
		    		j++;    	    	    		    			
	    	}

	    	if(externalIDFieldName == null && hasIDMapped)
	    		externalIDFieldName = IDFieldName;
	    			    		
			if(writeOperation == WriteOperation.UPSERT && externalIDFieldName == null)
			{
				throw new DataWriteException("You must map an External ID field for UPSERT");				
			}

			if(writeOperation == WriteOperation.UPDATE && !hasIDMapped)
			{
				throw new DataWriteException("You must map ID field for UPDATE");				
			}

			if(writeOperation == WriteOperation.INSERT && hasIDMapped)
			{
				throw new DataWriteException("cannot map ID field for INSERT");				
			}

			if(writeOperation == WriteOperation.DELETE && !hasIDMapped)
			{
				throw new DataWriteException("You must map ID field for DELETE");				
			}
			
	    	//Read data from the buffer and write to the target
	    	//Do not return from the method until buffer is empty or an exception/error occurs
			boolean hasMore = true;
			 while(hasMore) 
			 {
				//TODO Handle Batching of calls to SFDC instead of for every row
				 try
				 {
						List<Object> data = inputPipeline.get();
						
						//This is just a sanity check, data.length should always be equal to fieldList.size
						//The buffer only contains data for fields that are mapped.
						if(data.size() != fieldList.size())							
						{
				    		logger.warn("fieldList.size(): "+fieldList.size());
				    		logger.warn("data.length: "+data.size());

					    	for (com.sforce.dataset.connector.metadata.FieldType field : fieldList)
					    		logger.warn(field.getName());
							throw new FatalException("buffer and fieldList length do not match");
						}
						
						SObject sobj = new SObject();

						// The catalog name has the SObject name. It could be
						// Account.Owner but for write we dont support multiple
						// objects so it will always be just Account
						// The recordName could be SObject name or relationship name
				        sobj.setType(recordInfo.getName()); 
				    	for (int fieldIndex = 0; fieldIndex<fieldList.size();fieldIndex++)
				    	{
							//TODO Handle Related Objects			    		
				    		if(writeOperation == WriteOperation.UPDATE && (!isUpdateable[fieldIndex] || isAutoNumber[fieldIndex]))
				    			continue; //Skip field that cannot be updated
				    		
				    		if(writeOperation == WriteOperation.INSERT && (!isCreateable[fieldIndex] || isAutoNumber[fieldIndex]))
				    			continue; //Skip fields that cannot be inserted

				    		if(writeOperation == WriteOperation.UPSERT && (!isCreateable[fieldIndex] || isAutoNumber[fieldIndex] || !isUpdateable[fieldIndex]))
				    			continue; //Skip fields that cannot be inserted or updated

				    		logger.trace(fieldList.get(fieldIndex).getName()+": "+isCreateable[fieldIndex]+","+isUpdateable[fieldIndex]+","+isAutoNumber[fieldIndex]);
				    		
				    		sobj.setField(fieldList.get(fieldIndex).getName(), toSFDCType(data.get(fieldIndex), fieldList.get(fieldIndex).getClass(), fieldType[fieldIndex]));			    		
				    	}

				    	if(writeOperation != WriteOperation.INSERT && IDFieldIndex != -1)
				    			sobj.setId((String) data.get(IDFieldIndex));
				    	
				    	//TODO sobj.setFieldsToNull(fieldsToNull);
				    	
				    	if(writeOperation == WriteOperation.INSERT)
				    	{
				    		SaveResult[] results = partnerConnection.create(new SObject[] { sobj });				    		
				    		for(SaveResult sv:results)
				    		{ 	
				    			if(sv.isSuccess())
				    			{
			    					logger.info("Record {"+ sv.getId() + "} Inserted");
				    			}else
				    			{
			    					logger.error("Record {"+ sv.getId() + "} Insert Failed: " + getErrorMessage(sv.getErrors()));
				    			}
				    		}
			    	}else if(writeOperation == WriteOperation.UPDATE)
			    	{
				    		SaveResult[] results = partnerConnection.update(new SObject[] { sobj });
				    		for(SaveResult sv:results)
				    		{ 	
				    			if(sv.isSuccess())
				    			{
			    					logger.info("Record {"+ sv.getId() + "} Updated");
				    			}else
				    			{
			    					logger.error("Record {"+ sv.getId() + "} Update Failed: " + getErrorMessage(sv.getErrors()));
				    			}
				    		}
			    	}else if(writeOperation == WriteOperation.UPSERT)
				    {
				    		UpsertResult[] results = partnerConnection.upsert(externalIDFieldName, new SObject[] { sobj });
				    		for(UpsertResult sv:results)
				    		{ 	
				    			if(sv.isSuccess())
				    			{
			    					logger.info("Record {"+ sv.getId() +"} " + (sv.isCreated()? "Inserted":"Updated"));
				    			}else
				    			{
			    					logger.error("Record {"+ sv.getId() + "} Upsert Failed: " + getErrorMessage(sv.getErrors()));
				    			}
				    		}
				    	}else if(writeOperation == WriteOperation.DELETE)
					    {
					    		DeleteResult[] results = partnerConnection.delete(new String[]{(String) data.get(IDFieldIndex)});
					    		for(DeleteResult sv:results)
					    		{ 	
					    			if(sv.isSuccess())
					    			{
				    					logger.info("Record {"+ sv.getId() + "} Deleted");
					    			}else
					    			{
				    					logger.error("Record {"+ sv.getId() + "} Delete Failed: " + getErrorMessage(sv.getErrors()));
					    			}
					    		}
					    }else
					    {
							throw new FatalException("Invalid WriteOperation {"+writeOperation+"}");					    	
					    }					    
				    						    					    				    	
					} catch (ConnectionException e) {
						e.printStackTrace();
						throw new DataWriteException(e.toString());						
					}catch(IndexOutOfBoundsException ioobe){
						break;
					}
    		}//End While
		}catch (Throwable ex) {
			ex.printStackTrace();
			throw new DataWriteException(ex.toString());
		} finally {
			
		}
	        
    }


    private static int getPrecision(com.sforce.soap.partner.Field fld,Class<?> infaClazz) 
    {
    	int fldPrecision = 0;

		if(!infaClazz.getCanonicalName().equals(String.class.getCanonicalName()) 
				&&	!infaClazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName())
				&&	!infaClazz.getCanonicalName().equals(byte[].class.getCanonicalName()))
		{
			//To use defaults for all other types set to -1
			return -1;
		}
		
   	 	if(String.class.isAssignableFrom(infaClazz))
   	 	{
   	 		if(FieldType.base64.equals(fld.getType())) 
   	 		{
        		fldPrecision =  MAX_BASE64_LENGTH;
   	 		}else
   	 		{
	        	fldPrecision =  fld.getLength();
   	 		}

        	if(fldPrecision <= 0)
        	{
            	logger.warn("SFDC Field {"+fld.getName()+"} of type {"+fld.getType()+"} has length {"+fld.getLength()+"}");
            	logger.warn("SFDC Field Details: {"+fld.toString()+"}");
        		fldPrecision = 255;
        	}

   	 		return fldPrecision;
   	 	} else     	
        if(BigDecimal.class.isAssignableFrom(infaClazz))
       	{
        	if(fld.isCalculated())
        		fldPrecision =  MAX_DECIMAL_PRECISION;
        	else
        		fldPrecision =  fld.getPrecision();         

        	if(fldPrecision <= 0)
        	{
            	logger.warn("SFDC Field {"+fld.getName()+"} of type {"+fld.getType()+"} has precision {"+fld.getPrecision()+"}");
            	logger.warn("SFDC Field Details: {"+fld.toString()+"}");
        		fldPrecision = MAX_DECIMAL_PRECISION;
        	}

        	return fldPrecision;
       	}if(byte[].class.isAssignableFrom(infaClazz))
        {
        	fldPrecision = fld.getByteLength();
        	if(fldPrecision <= 0)
        		fldPrecision = fld.getLength();
        	if(fldPrecision <= 0)
        		fldPrecision = fld.getPrecision();
        	if(fldPrecision <= 0)
        		fldPrecision = fld.getDigits();        	        	

        	if(fldPrecision <= 0)
        	{
            	logger.warn("SFDC Field {"+fld.getName()+"} of type {"+fld.getType()+"} has length {"+fld.getLength()+"}");
            	logger.warn("SFDC Field Details: {"+fld.toString()+"}");
        		fldPrecision = 255;
        	}

        	return fldPrecision;
        }else //We should never hit this case
        {
        	logger.warn("Unusual SFDC Field Details: {"+fld.toString()+"}");

        	fldPrecision = fld.getByteLength();
        	if(fldPrecision <= 0)
        		fldPrecision = fld.getLength();
        	if(fldPrecision <= 0)
        		fldPrecision = fld.getPrecision();
        	if(fldPrecision <= 0)
        		fldPrecision = fld.getDigits();        	        	

        	if(fldPrecision <= 0)
        	{
            	logger.warn("SFDC Field {"+fld.getName()+"} of type {"+fld.getType()+"} has length {"+fld.getLength()+"}");
        		fldPrecision = 255;
        	}        	
        	return fldPrecision;
        }        
    }	


    private static int getScale(com.sforce.soap.partner.Field fld, Class<?> infaClazz) 
    {
    	int fldScale = 0;
		if(!infaClazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName()))
		{
			//To use defaults for all other types set to -1
			return -1;
		}
		
  		fldScale =  fld.getScale();       
    	if(fldScale <= 0)
    	{
        	logger.info("SFDC Field {"+fld.getName()+"} of type {"+fld.getType()+"} has scale {"+fld.getScale()+"}");
        	logger.debug("SFDC Field Details: {"+fld.toString()+"}");
       		fldScale = 0;
    	}
    	return fldScale;
    }    	


	private static Object toSFDCType(Object value, @SuppressWarnings("rawtypes") Class clazz, String fieldType) throws DataConversionException
	{
		if(value==null)
			return null;

		if(value instanceof BigDecimal)
		{
			return ((BigDecimal)value).doubleValue();
		}else if(value instanceof Timestamp)
		{
			 Calendar calendar = Calendar.getInstance();
			 calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
			 calendar.setTimeInMillis(((Timestamp)value).getTime());
			return calendar;
		}else if(value instanceof String)
		{
			if(fieldType.equals(com.sforce.soap.partner.FieldType.base64.toString()))
				return Base64.decodeBase64((String) value);
			else
				return value;
		}
		
		return value;
	}
	

	private static String generateSOQL(ObjectType recordInfo,List<com.sforce.dataset.connector.metadata.FieldType> fieldList, long pagesize,String filter)
			throws DataConversionException
	{
		StringBuilder soql = new StringBuilder("SELECT ");
		int i = 0;
		for (com.sforce.dataset.connector.metadata.FieldType field : fieldList) {
			if (i > 0)
				soql.append(", ");

			soql.append(field.getFullyQualifiedName());
			i++;
		}

		soql.append(" FROM ");

		String topLevelSObjectName = getTopLevelSObjectName(recordInfo.getFullyQualifiedName());
		soql.append(topLevelSObjectName);

		if(filter!= null && !filter.isEmpty())
			soql.append(" WHERE "+filter);
		
		if (pagesize>0)
			soql.append(" LIMIT " + pagesize);
		
		return soql.toString();
	}


	/*
	private static String generateWhereClause(List<FilterInfo> filterInfoList,
			AdvancedFilterInfo advancedFilterInfo, boolean isLookup,
			HashMap<String, JavaDataType> fieldNameJDTMap)
			throws DataConversionException {
		StringBuffer query = new StringBuffer();
		if (advancedFilterInfo != null
				&& advancedFilterInfo.getFilterCondition() != null
				&& !advancedFilterInfo.getFilterCondition().isEmpty()) {
			query.append(" WHERE "
					+ formatLastRunTimeStringInQuery(advancedFilterInfo
							.getFilterCondition()));
		} else if (filterInfoList != null && !filterInfoList.isEmpty()) {
			int cnt = 0;
			for (FilterInfo filterInfo : filterInfoList) {
				if (filterInfo.getField() != null
						&& filterInfo.getValues() != null) {
					
					logger.info("\nfilterInfo.getOperator(): "
							+ filterInfo.getOperator());
					logger.info("filterInfo.getField().getUniqueName(): "
									+ filterInfo.getField().getUniqueName());
					logger.info("filterInfo.getValues(): "
							+ filterInfo.getValues()+"\n");

					if (cnt != 0)
						query.append(" AND ");
					else
						query.append(" WHERE ");					

					// This will be either Account.ID for parent for Owner.Name for related records 					
					String columnName = filterInfo.getField().getUniqueName();

					String clause = EQUALS_OPERATOR;
					if (filterInfo.getOperator() != null)
						clause = filterOperatorMap.get(filterInfo.getOperator().name());

					clause = clause.replaceAll(Pattern.quote("{0}"),
							Matcher.quoteReplacement(columnName));

					if (filterInfo.getValues().isEmpty()
							|| filterInfo.getValues().get(0) == null) {
						clause = clause.replaceAll(Pattern.quote("{1}"),
								Matcher.quoteReplacement("null"));
					} else {
						
						String value = (String) filterInfo.getValues().get(0);

						// If only FieldInfo included JavaDataType for the Field
						// then we would have been more intelligent around
						// adding quotes for literal values.
						

						// Check if field is datetime or numeric or boolean
						JavaDataType jdt = fieldNameJDTMap.get(columnName);

						// This approach will fail if String field has numeric
						// values like AutoNumber field Type
						if (jdt == null)
							jdt = determineJavaDataTypeFromValue(columnName,value);

						logger.info("JavaDataType: "+ jdt);
						
						switch (jdt) {
						case JAVA_BOOLEAN:
							if (value.equals("1"))
								value = "TRUE";
							else
								value = "FALSE";
							break;
						case JAVA_TIMESTAMP:
							int timeIndex = value.indexOf("00:00:00");
							if (timeIndex != -1 && !value.contains("1970-01-01"))
								value = getFormatedDate(value.substring(0, timeIndex));
							else
								value = getFormatedDateTime(value);
							break;
						default:
							break;
						}
						value = quoteLiteral(value, "'", "\\", jdt);
						clause = clause.replaceAll(Pattern.quote("{1}"),Matcher.quoteReplacement(value));
					}
					query.append(clause);
					cnt++;
				}
			}
		}
		return query.toString();
	}
	*/
	
/*
	private  static String quoteLiteral(String literalValue,String quoteChar,String escapeChar,JavaDataType jdt) 
	{
		if (literalValue == null)
			return null;
		
		if(quoteChar==null || quoteChar.isEmpty() || quoteChar.equalsIgnoreCase("DEFAULT"))
			quoteChar = "'";

		if(escapeChar==null || escapeChar.isEmpty() || escapeChar.equalsIgnoreCase("DEFAULT"))
			escapeChar = "\\";

		if(jdt==null)
			jdt = JavaDataType.JAVA_STRING;
		
		if(literalValue.length() > 1 && literalValue.startsWith(quoteChar) && literalValue.endsWith(quoteChar))
			return literalValue;

		if(jdt ==  JavaDataType.JAVA_STRING)
			return String.format("%s%s%s", quoteChar, literalValue.replaceAll(Pattern.quote(quoteChar), Matcher.quoteReplacement(escapeChar+quoteChar)), quoteChar);
		else		
			return literalValue;		
	}
*/
	

/*
	public static synchronized JavaDataType determineJavaDataTypeFromValue(String columnName, String value)
	{
		logger.info("SFDCUtils.determineJavaDataTypeFromValue("+columnName+","+value+")");
		// If only FieldInfo included JavaDataType for the Field
		// then we would have been more intelligent around
		//  adding quotes.
		// This approach will fail if String field has numeric
		// values like AutoNumber field Type

    	//Check if field is datetime or numeric or boolean
		//This is nothing but a Hack. It is not guaranteed to work
		//Alternative is for the user to use Advanced Filters but that
		//Will not work for Lookups
    	if(NumberUtils.isNumber(value))
    	{
    		if(columnName.startsWith("Is") || columnName.contains(".Is"))
    		{
    			if(value.equals("0"))
    			{
        			return JavaDataType.JAVA_BOOLEAN;
    			}else if(value.equals("1"))
    			{
        			return JavaDataType.JAVA_BOOLEAN;
    			}
    		}
    		return JavaDataType.JAVA_BIGDECIMAL;
    	}else if($lastRunTimePattern.matcher(value).matches())
    	{
    		return JavaDataType.JAVA_TIMESTAMP;
    	}else if(timeStampPattern.matcher(value).matches())
    	{
    		return JavaDataType.JAVA_TIMESTAMP;
    	}else if($lastRunDatePattern.matcher(value).matches())
    	{
    		return JavaDataType.JAVA_TIMESTAMP;
    	}else
    		return JavaDataType.JAVA_STRING;    				
	}
*/

	public static synchronized String formatLastRunTimeStringInQuery(String soqlQuery) throws DataConversionException
	{
		  boolean foundLastRunTime = false;
		  StringBuffer result = new StringBuffer();
	    Matcher matcher = $lastRunTimePattern.matcher(soqlQuery);
	    while ( matcher.find() ) {
	    	foundLastRunTime = true;
			matcher.appendReplacement(result, getFormatedDateTime(matcher.group(1)));			
	    }
	    matcher.appendTail(result);	
		logger.debug("foundLastRunTime: "+foundLastRunTime);
	    return result.toString();
	}

	

//	public static synchronized String formatLastRunDateStringInQuery(String soqlQuery) throws DataConversionException
//	{
//		  boolean foundLastRunDate = false;
//		  StringBuffer result = new StringBuffer();
//	    Matcher matcher = $lastRunDatePattern.matcher(soqlQuery);
//	    while ( matcher.find() ) {
//	    	foundLastRunDate = true;
//			matcher.appendReplacement(result, getFormatedDate(matcher.group(1)));			
//	    }
//	    matcher.appendTail(result);	
//		logger.debug("foundLastRunDate: "+foundLastRunDate);
//	    return result.toString();
//	}


	private static synchronized String getFormatedDateTime(String infaTimeStamp) throws DataConversionException
	{
			Date filterdate = null;
			if(infaTimeStamp!=null && !infaTimeStamp.isEmpty())
			{
				try {
					
					infaTimeStamp = infaTimeStamp.replaceAll("'", "");
					
					//First check if there is a millisecond value in the string
					int dotIndex = infaTimeStamp.indexOf('.');
					if(dotIndex != -1 && dotIndex < (infaTimeStamp.length()-1))
					{
						filterdate = lastRunTimeDateWithMilliSecondsFormat.parse(infaTimeStamp);						
					}else
					{
						filterdate = lastRunTimeDateFormat.parse(infaTimeStamp);
					}
				} catch (ParseException e) {
					e.printStackTrace();
					throw new DataConversionException("Invalid Timestamp in Filter {"+infaTimeStamp+"}");
				}
			}

			if(filterdate!= null)
			{
				return sfdcDateTimeFormat.format(filterdate);
			}
			return infaTimeStamp;
	}



//	private static synchronized String getFormatedDate(String infaDateString) throws DataConversionException
//	{
//			Date filterdate = null;
//			if(infaDateString!=null && !infaDateString.isEmpty())
//			{
//				try {
//					filterdate = lastRunDateFormat.parse(infaDateString.replaceAll("'", ""));
//				} catch (ParseException e) {
//					e.printStackTrace();
//					throw new DataConversionException("Invalid date {"+infaDateString+"}");
//				}
//			}
//
//			if(filterdate!= null)
//			{
//				return sfdcDateFormat.format(filterdate);
//			}
//			return infaDateString;
//	}


	private static String getErrorMessage(com.sforce.soap.partner.Error[] errors)
	{
		StringBuffer strBuf = new StringBuffer();
		for(com.sforce.soap.partner.Error err:errors)
		{
		      strBuf.append(" statusCode={");
		      strBuf.append(DatasetUtils.getCSVFriendlyString(com.sforce.ws.util.Verbose.toString(err.getStatusCode()))+"}");
		      strBuf.append(" message={");
		      strBuf.append(DatasetUtils.getCSVFriendlyString(com.sforce.ws.util.Verbose.toString(err.getMessage()))+"}");
		      if(err.getFields()!=null && err.getFields().length>0)
		      {
			      strBuf.append(" fields=");
			      strBuf.append(DatasetUtils.getCSVFriendlyString(com.sforce.ws.util.Verbose.toString(err.getFields())));
		      }
		}
		return strBuf.toString();
	}
	
	

	private static Object getFieldValueFromQueryResult(String fieldName,SObject record)
	{
		logger.trace("getField("+fieldName+")");
		
		if(fieldName==null)
			return null;
		
		if(record==null)
			return null;
		
		if(fieldName.indexOf('.')==-1)
		{
			return record.getField(fieldName);			
		}else
		{
			String[] levels = fieldName.split("\\.");
			if(levels.length>2)
			{
				Object cur = record;
				for(int j=1;j<levels.length;j++)
				{
					cur = ((XmlObject)cur).getField(levels[j]);
					if(cur instanceof XmlObject)
						continue;
					else
						break;
				}
				return cur;
			}else if(levels.length==2)
			{
				return record.getField(levels[1]);			
			}else if(levels.length==1)
			{
				return record.getField(levels[0]);
			}else
			{
				return record.getField(fieldName);
			}
		}
	}


	private static String getTopLevelSObjectName(String fullyQualifiedObjectName)
	{
		String topLevelSOBject = fullyQualifiedObjectName;
		if(fullyQualifiedObjectName!=null && !fullyQualifiedObjectName.isEmpty())
		{
			// Lets try and parse catalog name and get Top level object
			String[] objectLevels = fullyQualifiedObjectName.split("\\.");
			if(objectLevels!= null && objectLevels.length>0)
				topLevelSOBject = objectLevels[0];
		}
		return topLevelSOBject;
	}
	
}
