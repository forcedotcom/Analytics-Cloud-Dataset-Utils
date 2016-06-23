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

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.XmlObject;

/**
 * The Class SfdcUtils.
 */
public class SfdcUtils {

	
	/** The Constant MAX_BASE64_LENGTH. */
	private static final int MAX_BASE64_LENGTH = 7 * 1024 * 1024; 
	
	/** The Constant MAX_DECIMAL_PRECISION. */
	private static final int MAX_DECIMAL_PRECISION = 38;

	/** The Constant sfdcDateTimeFormat. */
	private static final  SimpleDateFormat sfdcDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	static
	{
		sfdcDateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	/** The Constant sfdcDateFormat. */
	private static final  SimpleDateFormat sfdcDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	static
	{
		sfdcDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	
	//SFDC Object that are not supported in flow
/** The excluded objects. */
	//	static List<String> excludedObjects = Arrays.asList(new String[]{"UserRecordAccess","Vote","ContentDocumentLink","IdeaComment","UserProfile","AccountPartner","OpportunityPartner","CaseStatus","SolutionStatus","TaskStatus","LeadStatus","ContractStatus","OpportunityStage","PartnerRole","AccountHistory","ActivityHistory","CaseHistory","ContactHistory","ContentDocumentHistory","ContentVersionHistory","ContractHistory","LeadHistory","OrderHistory","OrderItemHistory","OpportunityFieldHistory","Pricebook2History","ProcessInstanceHistory","SolutionHistory","","AccountFeed","AssetFeed","CampaignFeed","CaseFeed","CollaborationGroupFeed","ContactFeed","ContentDocumentFeed","ContractFeed","DashboardFeed","EventFeed","LeadFeed","OrderFeed","OpportunityFeed","OrderItemFeed","Product2Feed","ReportFeed","TaskFeed","SolutionFeed","TopicFeed","UserFeed","DashboardComponentFeed","CaseTeamMember","CaseTeamRole","CaseTeamTemplate","CaseTeamTemplateMember","CaseTeamTemplateRecord","","AcceptedEventRelation","DeclinedEventRelation","UndecidedEventRelation","OpenActivity","TaskPriority","CombinedAttachment","NoteAndAttachment","OwnedContentDocument","AttachedContentDocument","Interviewer__Share","Position__Share","Applicant__Share","UserRecordAccess","RecentlyViewed","Name","WebLinkLocalization","RecordTypeLocalization","ScontrolLocalization","CategoryNodeLocalization","AggregateResult"});
	static List<String> excludedObjects = Arrays.asList(new String[]{"AcceptedEventRelation","AuthProvider","BrandTemplate","Calendar","ConnectedApplication","ContentWorkspace","ContentWorkspaceDoc","CorsWhitelistEntry","CustomNotDeployed__c", "CustomNotDeployed__OwnerSharingRule", "DeclinedEventRelation","EmailDomainKey","EmailServicesAddress","EmailServicesFunction","EmailTemplate","EnvironmentHub","EnvironmentHubInvitation","EnvironmentHubMemberRel","EventRecurrenceException","EventRelation","FeedPollChoice","FeedPollVote","LoginHistory","OrgWideEmailAddress","OutboundField","PackageLicense","PartnerNetworkSyncLog","SelfServiceUser","SsoUserMapping", "TaskRecurrenceException","UndecidedEventRelation","UserLogin","UserPackageLicense","WebLink","WebLinkLocalization","CollaborationGroupRecord","ContentDocumentLink", "IdeaComment", "Vote"});
	
	/** The Constant sfdcFieldTypeToJavaClassMap. */
	static final HashMap<FieldType,Class<?>> sfdcFieldTypeToJavaClassMap = new HashMap<FieldType,Class<?>>();
	static {		
		sfdcFieldTypeToJavaClassMap.put(FieldType.string, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType._boolean, java.lang.Boolean.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType._int, java.lang.Integer.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType._double, java.math.BigDecimal.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.date, java.sql.Timestamp.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.datetime, java.sql.Timestamp.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.base64, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.id, java.lang.String.class);	
		sfdcFieldTypeToJavaClassMap.put(FieldType.reference, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.currency, java.math.BigDecimal.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.textarea, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.percent, java.math.BigDecimal.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.phone, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.url, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.email, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.combobox, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.encryptedstring, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.anyType, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.datacategorygroupreference, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.time, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.picklist, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.multipicklist, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.anyType, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.location, java.lang.String.class);
	}	
	
	/** The Constant excludedSfdcFieldTypeMap. */
	static final HashMap<FieldType,Class<?>> excludedSfdcFieldTypeMap = new HashMap<FieldType,Class<?>>();
	static {
		excludedSfdcFieldTypeMap.put(FieldType.address, java.lang.String.class);
		excludedSfdcFieldTypeMap.put(FieldType.encryptedstring, java.lang.String.class);
		excludedSfdcFieldTypeMap.put(FieldType.base64, java.lang.String.class);
		excludedSfdcFieldTypeMap.put(FieldType.location, java.lang.String.class);		
		excludedSfdcFieldTypeMap.put(FieldType.datacategorygroupreference, java.lang.String.class);
	}
	
	
	/**
	 * Gets the object list.
	 *
	 * @param partnerConnection the partner connection
	 * @param pattern the pattern
	 * @param isWrite the is write
	 * @return the object list
	 * @throws ConnectionException the connection exception
	 */
	public static Map<String,String> getObjectList(PartnerConnection partnerConnection, Pattern pattern, boolean isWrite) throws ConnectionException
	{

		if (pattern == null || pattern.pattern().isEmpty())
			pattern = Pattern.compile(".*");

		LinkedHashMap<String,String> backendObjectInfoList = new LinkedHashMap<String,String>();

			// Make the describeGlobal() call
			DescribeGlobalResult describeGlobalResult = partnerConnection.describeGlobal();

			// Get the sObjects from the describe global result
			DescribeGlobalSObjectResult[] sobjectResults = describeGlobalResult.getSobjects();

			// Write the name of each sObject to the console
			for (DescribeGlobalSObjectResult sObjectResult : sobjectResults) 
			{	
				// Skip Objects that are deprecated
				if (sObjectResult.isDeprecatedAndHidden())
					continue;

				if(excludedObjects.contains(sObjectResult.getName()))
				{
//					System.out.println("Skipping object {"+sObjectResult.getName()+"}");
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

				if(isWrite){
					if((!sObjectResult.isUpdateable() || !sObjectResult.isCreateable() || !sObjectResult.isDeletable()))
					{
						continue;
					}
				}
				backendObjectInfoList.put(sObjectResult.getName(),sObjectResult.getName());
			}			

		return backendObjectInfoList;

	}

	

	/**
	 * Gets the related object list.
	 *
	 * @param partnerConnection the partner connection
	 * @param primaryObject the primary object
	 * @param primaryObjectType the primary object type
	 * @param isWrite the is write
	 * @return the related object list
	 * @throws ConnectionException the connection exception
	 */
	public static Map<String,String> getRelatedObjectList(
			PartnerConnection partnerConnection, String primaryObject,String primaryObjectType, boolean isWrite) throws ConnectionException  {

		LinkedHashMap<String,String> backendObjectInfoList = new LinkedHashMap<String,String>();
		
		// Salesforce related objects can be many levels deep for Example
		// 'Contact.Account.Owner'
			DescribeSObjectResult dsr = partnerConnection.describeSObject(primaryObjectType);
			// Now, retrieve metadata for each field
			for (int i = 0; i < dsr.getFields().length; i++) 
			{
				// Get the field
				com.sforce.soap.partner.Field field = dsr.getFields()[i];

				// Skip fields that are deprecated
				if (field.isDeprecatedAndHidden())
					continue;
				
				//Get the parents fully qualified name for example Contact, or Contact.Account
				String parentFullyQualifiedName = primaryObject;
			
				//We are only interested in fields that are Relationships (Foreign Keys)
				if(field.getRelationshipName()!=null && !field.getRelationshipName().isEmpty() && field.getReferenceTo() != null && field.getReferenceTo().length!=0)
				{
					for (String relatedSObjectType : field.getReferenceTo()) 
					{
//						if (relatedSObjectType.isDeprecatedAndHidden())
//							continue;
//
						if(excludedObjects.contains(relatedSObjectType))
						{
							System.out.println("Skipping object {"+relatedSObjectType+"}");
							continue;
						}

						// This is what shows in the related object list object
						// list drop down when user clicks on get Siblings
						String objectName = field.getRelationshipName();

						// This will be used by this code to build SFDC SOQL
						// I need it because when we are querying
						// multiple objects we need to know the fully
						// qualified path of the object for example
						// if the parent is Account and the relationship name is Owner
						// Then fully qualified name will be Account.Owner 
						String fullyQualifiedObjectName = parentFullyQualifiedName + "." + objectName;
						
						// The Object Label, SFDC has polymorphic relationships
						// for example Account.Owner Can be related to User or
						// Group. We need to give users a way to choose the
						// correct Relationship.
						@SuppressWarnings("unused")
						String objectLabel = objectName + "(" + relatedSObjectType + ")";

						// Add it to the list
						backendObjectInfoList.put(fullyQualifiedObjectName, relatedSObjectType);
					}
				}
			}
		return backendObjectInfoList;
	}



	
	/**
	 * Gets the field list.
	 *
	 * @param sObjectType the s object type
	 * @param partnerConnection the partner connection
	 * @param isWrite the is write
	 * @return the field list
	 * @throws ConnectionException the connection exception
	 */
	public static List<com.sforce.dataset.loader.file.schema.ext.FieldType> getFieldList(String sObjectType,
			PartnerConnection partnerConnection, boolean isWrite) throws ConnectionException
	{

			List<com.sforce.dataset.loader.file.schema.ext.FieldType> fieldList = new ArrayList<com.sforce.dataset.loader.file.schema.ext.FieldType>();
	    	com.sforce.dataset.Preferences userPref = DatasetUtilConstants.getPreferences(partnerConnection.getUserInfo().getOrganizationId());

			
			DescribeSObjectResult dsr = partnerConnection.describeSObject(sObjectType);			
			if (dsr != null) 
			{
				// Now, retrieve metadata for each field
				LinkedHashMap<String, Field> labels = new LinkedHashMap<String, Field>();				
				for (int i = 0; i < dsr.getFields().length; i++) 
				{
					// Get the field
					com.sforce.soap.partner.Field field = dsr.getFields()[i];

					// Skip fields that are deprecated
					if (field.isDeprecatedAndHidden())
						continue;
					
					if(excludedSfdcFieldTypeMap.containsKey(field.getType()))
						continue;
					
					if(sObjectType.equals("User"))
					{
						if(field.getName().equals("LastPasswordChangeDate") || field.getName().equals("IsBadged"))
							continue;
					}
					
					if(sObjectType.equals("Profile"))
					{
						if(field.getName().equals("PermissionsEditTask") || field.getName().equals("PermissionsEditEvent"))
							continue;
					}

		//Prevent duplicate label error
					
					if(labels.containsKey(field.getLabel()))
					{
						System.out.println("{"+field.getName()+"} has duplicate label matching field {"+labels.get(field.getLabel()).getName()+"}");
//						continue;
					}
					
					labels.put(field.getLabel(), field);
										

					// Get the Java class corresponding to the SFDC FieldType
					Class<?> clazz = getJavaClassFromFieldType(field.getType());			        	 			        	 

					
					// Determine the field Precision
					int precision = getPrecision(field, clazz);
					// Determine the field Scale
					int scale = getScale(field, clazz);

					com.sforce.dataset.loader.file.schema.ext.FieldType bField = null;
					if(clazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName()) || clazz.getCanonicalName().equals(Integer.class.getCanonicalName()))
					{
						bField = com.sforce.dataset.loader.file.schema.ext.FieldType.GetMeasureKeyDataType(field.getName(), precision, scale, 0L);
					}else if(clazz.getCanonicalName().equals(java.sql.Timestamp.class.getCanonicalName()))
					{
						bField = com.sforce.dataset.loader.file.schema.ext.FieldType.GetDateKeyDataType(field.getName(), "MM/dd/yyyy hh:mm:ss a", null, userPref);						
					}else
					{
						if(field.getType().equals(FieldType.multipicklist))
						{
							bField = com.sforce.dataset.loader.file.schema.ext.FieldType.GetStringKeyDataType(field.getName(), null, null);
						}else
						{
							bField = com.sforce.dataset.loader.file.schema.ext.FieldType.GetStringKeyDataType(field.getName(), ";", null);
						}
					}

					// Set the Business Name (Name used in UI)
					if(bField==null)
					{
						System.out.println("field: "+ field);
					}
					bField.setLabel(field.getLabel());
					
					bField.setDescription(field.getInlineHelpText());
					
					// In case of multiple source Objects the column names could
					// clash for example account and contact both have columns
					// named 'id' Therefore we prefix the column name with the
					// catalog name to make it unique For example Account.Id and
					// Account.Owner.Id
					bField.setFullyQualifiedName(sObjectType+"."+field.getName());
					
										
					// If the field is primary key
					if (field.getType().equals(FieldType.id)
							|| field.isExternalId())
						bField.setUniqueId(true);
			    		
		    		fieldList.add(bField);
				}
			}
			return fieldList;
	}	   
	   

		

	/**
	 * Read.
	 *
	 * @param partnerConnection the partner connection
	 * @param recordInfo the record info
	 * @param fieldList the field list
	 * @param pagesize the pagesize
	 * @param dataDir the data dir
	 * @return true, if successful
	 * @throws ConnectionException the connection exception
	 * @throws UnsupportedEncodingException the unsupported encoding exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static boolean read(PartnerConnection partnerConnection,String recordInfo,
			List<com.sforce.dataset.loader.file.schema.ext.FieldType> fieldList,
			long pagesize, File dataDir) throws 
		ConnectionException, UnsupportedEncodingException, IOException
			 {
			// These debug statements should help you understand what is being
			// passed back to your calls. You can comment these out if you like
			BufferedOutputStream bos = null;
//			CsvWriter writer = null;
			CsvWriter writer = null;
			File csvFile = new File(dataDir,recordInfo+".csv");
			
			if(pagesize==0)
				pagesize = 1000;
			
			partnerConnection.setQueryOptions(2000);
			if (pagesize>0)
				partnerConnection.setQueryOptions((int) pagesize);

			//Generate the SOQL using the FieldList and RecordInfo
			String soqlQuery = generateSOQL(recordInfo, fieldList, pagesize)  ;
			System.out.println("SOQL: "+soqlQuery);
			try {
				boolean canWrite = false;
				while(!canWrite)
				{
					try
					{
						bos = new BufferedOutputStream(new FileOutputStream(csvFile));
						canWrite = true;
						bos.close();
						bos = null;
					}catch(Throwable t)
					{	
//						System.out.println(t.getMessage());
						canWrite = false;
						DatasetUtils.readInputFromConsole("file {"+csvFile+"} is open in excel please close it first, press enter when done: ");
					}
				}
				
//				writer = new CsvListWriter(new FileWriter(csvFile),CsvPreference.STANDARD_PREFERENCE);
		        writer = new CsvWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile), StringUtilsExt.utf8Charset),DatasetUtilConstants.DEFAULT_BUFFER_SIZE),',','"');

				List<String> hdr = new LinkedList<String>();
				for(com.sforce.dataset.loader.file.schema.ext.FieldType field:fieldList)
				{
					hdr.add(field.getName());
				}
				writer.writeRecord(hdr);
			} catch (IOException e) {
				e.printStackTrace();
			}

			
			//Query SFDC
			QueryResult qr = partnerConnection.query(soqlQuery);

			int rowsSoFar = 0;
			boolean done = false;
			if (qr.getSize() > 0) 
			{
				while (!done) {
					SObject[] records = qr.getRecords();
					for (int i = 0; i < records.length; ++i) {
						String[] rowData = new String[fieldList.size()];
						for (int var = 0; var < fieldList.size(); var++) {
							String fieldName = fieldList.get(var).getFullyQualifiedName(); //This is full path of the field
							Object value = getFieldValueFromQueryResult(fieldName,records[i]);
							//Object value = records[i].getField(fieldName);
							if (value != null) {
								// Convert the value to a type from JavaDataType
								// first before setting it in rowData
								value = DatasetUtils.toJavaPrimitiveType(value);
							}
//							rowData[var] = value;
							if(value==null)
							{
								rowData[var] = null;
							}else
							{
								if(value instanceof Number)
								{
									rowData[var] = ((new BigDecimal(value.toString())).toPlainString());
								}else if(value instanceof Date)
								{
									rowData[var] = (sfdcDateTimeFormat.format((Date)value));
								}else
								{
									rowData[var] =  (value.toString());
								}
							}							
					}
						if(writer!=null)
						{
							try {
								writer.writeRecord(rowData);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						rowsSoFar++;
						// If preview, exit the while loop after pagesize is reached
						if (pagesize > 0 && i >= pagesize - 1)
							break;
					}

//					// If its preview exit when the first set is done even if
//					// pageSize is not reached
//					if (qr.isDone() || pagesize > 0) {
//						done = true;
					if (qr.isDone()) {
						done = true;
					} else {
						qr = partnerConnection.queryMore(qr.getQueryLocator());
					}
				}// End While
			}
			
			if(writer!=null)
				try {
					writer.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			if(bos!=null)
				try {
					bos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
    		System.out.println("Query returned {" + rowsSoFar + "} rows");				
    		System.out.println("Query results saved in file {" + csvFile + "}");				
			return true;
	}	
	
    

	/**
	 * Gets the precision.
	 *
	 * @param fld the fld
	 * @param sfdcClazz the sfdc clazz
	 * @return the precision
	 */
    private static int getPrecision(com.sforce.soap.partner.Field fld,Class<?> sfdcClazz) 
    {
    	int fldPrecision = 0;

		if(!sfdcClazz.getCanonicalName().equals(String.class.getCanonicalName()) 
				&&	!sfdcClazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName())
				&&	!sfdcClazz.getCanonicalName().equals(byte[].class.getCanonicalName()))
		{
			//To use defaults for all other types set to -1
			return -1;
		}
		
   	 	if(String.class.isAssignableFrom(sfdcClazz))
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
        		fldPrecision = 255;
        	}

   	 		return fldPrecision;
   	 	} else     	
        if(BigDecimal.class.isAssignableFrom(sfdcClazz))
       	{
        	if(fld.isCalculated())
        		fldPrecision =  MAX_DECIMAL_PRECISION;
        	else
        		fldPrecision =  fld.getPrecision();         

        	if(fldPrecision <= 0)
        	{
        		fldPrecision = MAX_DECIMAL_PRECISION;
        	}

        	return fldPrecision;
       	}if(byte[].class.isAssignableFrom(sfdcClazz))
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
        		fldPrecision = 255;
        	}

        	return fldPrecision;
        }else //We should never hit this case
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
        		fldPrecision = 255;
        	}        	
        	return fldPrecision;
        }        
    }	

    
    /**
     * Gets the scale.
     *
     * @param fld the fld
     * @param sfdcClazz the sfdc clazz
     * @return the scale
     */
    private static int getScale(com.sforce.soap.partner.Field fld, Class<?> sfdcClazz) 
    {
    	int fldScale = 0;
		if(!sfdcClazz.getCanonicalName().equals(BigDecimal.class.getCanonicalName()))
		{
			//To use defaults for all other types set to -1
			return -1;
		}
		
  		fldScale =  fld.getScale();       
    	if(fldScale <= 0)
    	{
       		fldScale = 0;
    	}
    	return fldScale;
    }    	

	
	
	/**
	 * Generate soql.
	 *
	 * @param recordInfo the record info
	 * @param fieldList the field list
	 * @param pagesize the pagesize
	 * @return the string
	 */
	private static String generateSOQL(String recordInfo,
			List<com.sforce.dataset.loader.file.schema.ext.FieldType> fieldList, long pagesize)
	{
//		HashMap<String, JavaDataType> fieldNameJDTMap = new HashMap<String, JavaDataType>();
		String topLevelSObjectName = getTopLevelSObjectName(recordInfo);
		int varLen =  topLevelSObjectName.length() + " FROM ".length() + (" LIMIT " + pagesize).length();
		StringBuilder soql = new StringBuilder("SELECT ");
		int i = 0;
		for (com.sforce.dataset.loader.file.schema.ext.FieldType field : fieldList) {
			
			if((soql.length()+(", " + field.getName()).length())>(20000-varLen))
			{
				System.out.println("Too many fields in object {"+topLevelSObjectName+"} truncating query to 20,000 chars");
				break;
			}

			if (i > 0)
				soql.append(", ");

			soql.append(field.getName());
			i++;
		}

		soql.append(" FROM ");

		// If the catalog name is Account.Owner then we will use Account
		// (Top level object) in the FROM clause
		soql.append(topLevelSObjectName);

		//If this is preview then limit result set for efficiency
		if (pagesize>0)
			soql.append(" LIMIT " + pagesize);
		
		return soql.toString();
	}

	
	/**
	 * Gets the error message.
	 *
	 * @param errors the errors
	 * @return the error message
	 */
	@SuppressWarnings("unused")
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
	
	
	/**
	 * Gets the field value from query result.
	 *
	 * @param fieldName the field name
	 * @param record the record
	 * @return the field value from query result
	 */
	public static Object getFieldValueFromQueryResult(String fieldName,SObject record)
	{
//		System.out.println("getField("+fieldName+")");
		
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


	/**
	 * Gets the top level s object name.
	 *
	 * @param fullyQualifiedObjectName the fully qualified object name
	 * @return the top level s object name
	 */
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
	
	/**
	 * Gets the java class from field type.
	 *
	 * @param fieldType the field type
	 * @return the java class from field type
	 */
	public static Class<?> getJavaClassFromFieldType(
			com.sforce.soap.partner.FieldType fieldType) {
		
		Class<?> clazz = sfdcFieldTypeToJavaClassMap.get(fieldType);

		if (clazz == null)
			clazz = java.lang.String.class;

		return clazz;
	}
	
	
}
