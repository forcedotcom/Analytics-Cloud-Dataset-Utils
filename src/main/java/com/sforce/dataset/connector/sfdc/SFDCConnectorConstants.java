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

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sforce.dataset.connector.ConnectorUtils;
import com.sforce.soap.partner.FieldType;


public class SFDCConnectorConstants {


//TODO - 1.1 INFA: generate UUID using the GenerateUUIDTest class and paste it here
	public static final String PLUGIN_UUID = "ee3a5510-573a-402a-a17d-8bd4220103ef";

//TODO - 1.2 INFA: Define alphanumeric replacement string for special characters 
//this is simple map between special characters and the corresponding replacement strings 
	private static final String[][] specialCharacterReplacementMap = {{" ","__bl__"}, {"?","__qu__"}, {":","__co__"}, {".","__do__"},{"-","__hi__"}};	
	
	private static final HashMap<FieldType,Class<?>> sfdcFieldTypeToJavaClassMap = new HashMap<FieldType,Class<?>>();
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
		sfdcFieldTypeToJavaClassMap.put(FieldType.time, java.sql.Timestamp.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.picklist, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.multipicklist, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.anyType, java.lang.String.class);
		sfdcFieldTypeToJavaClassMap.put(FieldType.location, java.lang.String.class);		
	}	

	
	public static String sanitizeName(String var)
	{		
		for(int i=0;i<SFDCConnectorConstants.specialCharacterReplacementMap.length;i++)
		{
			var = var.replaceAll(Pattern.quote(SFDCConnectorConstants.specialCharacterReplacementMap[i][0]), Matcher.quoteReplacement(SFDCConnectorConstants.specialCharacterReplacementMap[i][1]));
		}
		return var;
	}

	public static String unsanitizeName(String var)
	{		
		for(int i=0;i<SFDCConnectorConstants.specialCharacterReplacementMap.length;i++)
		{
			var = var.replaceAll(Pattern.quote(SFDCConnectorConstants.specialCharacterReplacementMap[i][1]), Matcher.quoteReplacement(SFDCConnectorConstants.specialCharacterReplacementMap[i][0]));
		}
		return var;
	}
	
	
	public static Class<?> getJavaClassFromFieldType(
			com.sforce.soap.partner.FieldType fieldType) {
		
		Class<?> clazz = SFDCConnectorConstants.sfdcFieldTypeToJavaClassMap
				.get(fieldType);

		if (clazz == null)
			clazz = java.lang.String.class;

		return ConnectorUtils.getAdjustedDataType(clazz);
	}
		
}
