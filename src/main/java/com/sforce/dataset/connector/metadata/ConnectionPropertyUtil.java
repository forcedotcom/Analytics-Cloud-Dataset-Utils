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
package com.sforce.dataset.connector.metadata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sforce.dataset.connector.IConnector;

public class ConnectionPropertyUtil {

	//private static final Logger logger = Logger.getLogger(ConnectionPropertyUtil.class);
	private static final Log logger = LogFactory.getLog(ConnectionPropertyUtil.class);
	
	public static List<ConnectionProperty> getConnectionProperties(IConnector connectionImpl) 
	{
		logger.debug("ConnectionPropertyUtil.getConnectionPropertys(): "+connectionImpl.getClass().getCanonicalName());
		List<ConnectionProperty> ConnectionPropertyList = new ArrayList<ConnectionProperty>();
		
		Field[] flds = connectionImpl.getClass().getFields();
		
		int cnt = 1;
		for(Field field:flds)
		{ 
			String name = field.getName();
			Class<?> type = field.getType();

			logger.debug("name: "+name);
			logger.debug("type: "+type);

			ConnectionPropertyType connectionPropertyType;
			String label = null;
			String description = null;
			boolean required = false;
			List<String> listValues = null;
			String defaultValue = null;							
			
			com.sforce.dataset.connector.annotation.ConnectionProperty cp = field.getAnnotation(com.sforce.dataset.connector.annotation.ConnectionProperty.class);
			if(cp != null)
			{
				try {		
					defaultValue =	BeanUtils.getSimpleProperty(connectionImpl, name);
					logger.debug("defaultValue: "+defaultValue);
				} catch (IllegalArgumentException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					e1.printStackTrace();
				} catch (InvocationTargetException e1) {
					e1.printStackTrace();
				} catch (NoSuchMethodException e1) {
					e1.printStackTrace();
				} 				
				
				
				ConnectionPropertyType pType = cp.type();
				if(pType == ConnectionPropertyType.BOOLEAN)
				{
					if(type.getName().equalsIgnoreCase(boolean.class.getName()))
					{
						connectionPropertyType = ConnectionPropertyType.BOOLEAN;
						if(defaultValue != null && defaultValue.equalsIgnoreCase("true"))
							defaultValue = "true";
						else
							defaultValue = "false";
					}else
					{
							throw new IllegalArgumentException("Illegal Type" + type +"for field {"+name+"}");
					}
				}else if(pType == ConnectionPropertyType.NUMERIC)
				{					
					if(type.getName().equalsIgnoreCase(int.class.getName()))
					{
						connectionPropertyType = ConnectionPropertyType.NUMERIC;
					}else if(type.getName().equalsIgnoreCase(long.class.getName()))
					{
						connectionPropertyType = ConnectionPropertyType.NUMERIC;
					}else if(type.getName().equalsIgnoreCase(double.class.getName()))
					{
						connectionPropertyType = ConnectionPropertyType.NUMERIC;
					}else
					{
							throw new IllegalArgumentException("Illegal Type" + type +"for field {"+name+"}");
					}
				}else if(pType == ConnectionPropertyType.STRING)
				{					
					if(type.getName().equalsIgnoreCase(String.class.getName()))
					{
						connectionPropertyType = ConnectionPropertyType.STRING;
					}else
					{
							throw new IllegalArgumentException("Illegal Type" + type +"for field {"+name+"}");
					}
				}else
				{
						throw new IllegalArgumentException("Illegal Type" + type +"for field {"+name+"}" );
				}

				if(cp.defaultValues() != null && cp.defaultValues().length>1)
				{
					listValues = Arrays.asList(cp.defaultValues());
					logger.debug("listValues: "+listValues);	
				}else
				{
					if(defaultValue != null)
					{
						listValues = new ArrayList<String>(1);
						listValues.add(defaultValue);
					}
				}
								
				required = cp.required();
				label = cp.label();				
			
				if(label == null || label.isEmpty() || containsSpecialCharacters(label))
				{
					throw new IllegalArgumentException("@ConnectionProperty label for field {"+name+"}  cannot contain special characters {"+label+")");
				}
				
				ConnectionProperty ConnectionProperty = new ConnectionProperty();
				ConnectionProperty.setId(cnt);
				ConnectionProperty.setName(name);
				ConnectionProperty.setLabel(label);
				ConnectionProperty.setType(connectionPropertyType);
				ConnectionProperty.setDescription(description);
				ConnectionProperty.setRequired(required);
				ConnectionProperty.setDefaultValues(listValues); //Required if Type is ConnectionPropertyType.LIST_TYPE
				ConnectionPropertyList.add(ConnectionProperty);
			
				cnt++;
			}
		}
		logger.debug(ConnectionPropertyList);
		if(ConnectionPropertyList.isEmpty())
		{
			logger.fatal("No Connection attributes are declared in class {"+connectionImpl+"}");
			throw new IllegalArgumentException("No Connection attributes are declared in class {"+connectionImpl+"}");
		}
		return ConnectionPropertyList;
	}

	
	public static void setConnectionPropertys(Object connectorImpl, Map<String, String> ConnectionPropertys) 
	{
		logger.debug("ConnectionPropertyUtil.setConnectionPropertys(): "+connectorImpl.getClass().getCanonicalName());

		Field[] flds = connectorImpl.getClass().getFields();
		for(Field field:flds)
		{ 
			String name = field.getName();
			Class<?> type = field.getType();

			logger.debug("name: "+name);
			logger.debug("type: "+type);

			
			com.sforce.dataset.connector.annotation.ConnectionProperty cp = field.getAnnotation(com.sforce.dataset.connector.annotation.ConnectionProperty.class);
			if(cp != null)
			{				
				try {					
					String label = cp.label();
					if(label != null && !label.isEmpty())
					{
						String value = ConnectionPropertys.get(label);
						if(value!=null)
						{							
							if(type.getCanonicalName().equals(boolean.class.getCanonicalName()))
							{
								if(value.equalsIgnoreCase(Boolean.TRUE.toString()) || value.equalsIgnoreCase("YES") || value.equalsIgnoreCase("1"))
									value = "true";
								else
									value = "false";										
							}
							//logger.debug("value: "+value);
							BeanUtils.setProperty(connectorImpl, name, value);							
						}else
						{
								if(cp.required())
									logger.error("Connection value for required Connection Attribute {"+name+"} is missing");
						}						
					}
				} catch (IllegalArgumentException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					e1.printStackTrace();
				} catch (InvocationTargetException e1) {
					e1.printStackTrace();
				} 								
			}			
		}
	}
	

  public static boolean containsSpecialCharacters(String str)
  {
    Pattern pat = Pattern.compile("[^a-z0-9 ]", 2);
    Matcher m = pat.matcher(str);
    return m.find();
  }	

}
