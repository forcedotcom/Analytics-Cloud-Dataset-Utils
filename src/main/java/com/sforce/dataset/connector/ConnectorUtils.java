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
package com.sforce.dataset.connector;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Date;
import java.sql.RowId;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.sforce.dataset.connector.exception.DataConversionException;
import com.sforce.dataset.connector.metadata.ConnectionProperty;
import com.sforce.dataset.connector.metadata.ConnectionPropertyUtil;


/**
 * @author pugupta
 *
 */
public class ConnectorUtils {
	
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

	public static final int EOF = -1;

	public static final char LF = '\n';

	public static final char CR = '\r';

	public static final char QUOTE = '"';

	public static final char COMMA = ',';
	

	public static Class<?> getAdjustedDataType(Class<?> clazz) {

		if(Date.class.isAssignableFrom(clazz))
			return java.sql.Timestamp.class;

		if(Calendar.class.isAssignableFrom(clazz))
			return java.sql.Timestamp.class;

		if (clazz.getName().equals("java.util.Date"))
			return java.sql.Timestamp.class;

		if (clazz.getName().equals("javax.xml.datatype.XMLGregorianCalendar"))
			return java.sql.Timestamp.class;

		// special case for handling XMLGregorianCalendar data type
		if (clazz.getName().equals("java.lang.Character"))
			return String.class;
		
		// special case for handling Time data type
		if (clazz.getName().equals("java.sql.Time"))
			return java.sql.Timestamp.class;

		if (clazz.getName().equals("java.sql.Date"))
			return java.sql.Timestamp.class;
		
		if(clazz.getName().equalsIgnoreCase("java.sql.Blob"))
			return byte[].class;

		if(clazz.getName().equalsIgnoreCase("java.sql.Clob"))
			return String.class;

		// non-wrapped primitive data type	
		return ClassUtils.primitiveToWrapper(clazz);
	}

	public static Object getAdjustedValue(Object value, String classCanonicalName) throws DataConversionException 
	{		
		if(value == null)
			return null;
		
		if(classCanonicalName==null)
			throw new DataConversionException("classCanonicalName cannot be null");
				
		return toJavaDataType(value, classCanonicalName);				
	}
	
			
		  public static String getClassLoaderClassPath(URLClassLoader loader) 
		  {			  
				StringBuffer strBuf = new StringBuffer();
				if(loader!=null)
				{
			        URL[] urls = loader.getURLs();
			        boolean first = true;
			        for(URL url: urls){
			        	if(!first)
			        		strBuf.append(";");
			        	strBuf.append(url.getFile());
			        	first = false;
			        }
				}
				return strBuf.toString();
		  }
		  
		 
		  public synchronized static boolean configureLog4j() 
		    {
		    	try
		    	{
			    	if (!isLog4jConfigured()) 
			        {
			            BasicConfigurator.configure();
			            Logger log = LogManager.getRootLogger();
			            if(log!=null)
			            	log.setLevel(Level.INFO);
				        else 
				        {
				            Enumeration<?> loggers = LogManager.getCurrentLoggers() ;
				            while (loggers.hasMoreElements()) {
				                Logger c = (Logger) loggers.nextElement();
				            	c.setLevel(Level.INFO);
				            }
				        }
			        }
				}catch(Throwable t){
					t.printStackTrace();
				}    
		    	return true;
		    }

		    public synchronized static void shutdownLog4j() {
		        if (isLog4jConfigured()) {
		            LogManager.shutdown();
		        }
		    }
		    
	    private static boolean isLog4jConfigured() {
	    	try
	    	{
		        Enumeration<?> appenders =  LogManager.getRootLogger().getAllAppenders();
		        if (appenders.hasMoreElements()) {
		            return true;
		        }
		        else {
		            Enumeration<?> loggers = LogManager.getCurrentLoggers() ;
		            while (loggers.hasMoreElements()) {
		                Logger c = (Logger) loggers.nextElement();
		                if (c.getAllAppenders().hasMoreElements())
		                    return true;
		            }
		        }
	    	}catch(Throwable t){
	    		t.printStackTrace();
	    	}    
	        return false;
	    }				  
		  
	    public static boolean isLetterOrDigit(String stringValue) {
		 for (char c : stringValue.toCharArray()) {
		      if (!Character.isLetterOrDigit(c))
		        return false;
		    }
		  return true;
	    }
	    
	    
	    public static byte[] toBytes(int i)
	    {
	      byte[] result = new byte[4];

	      result[0] = (byte) (i >> 24);
	      result[1] = (byte) (i >> 16);
	      result[2] = (byte) (i >> 8);
	      result[3] = (byte) (i /*>> 0*/);

	      return result;
	    }

		public static Properties loadProperties(File propFile) throws FileNotFoundException 
		{
			Properties props = new Properties();
	    	try{
	    		props.load(new FileInputStream(propFile));	    		
	    	} catch (IOException ex) {
	    		throw new FileNotFoundException("Unable to load propertiess file: "+propFile);
	    	}
			return props;
		}

	    
		public static Properties loadProperties(String propFileName) throws FileNotFoundException 
		{
			Properties props = new Properties();
			ClassLoader loader = ConnectorUtils.class.getClassLoader();
	        if (loader == null) {
		    	 loader = Thread.currentThread().getContextClassLoader();
	        }
	    	try{
	    		InputStream propStream = loader.getResourceAsStream(propFileName);
	    		if(propStream!=null)
	    			props.load(propStream);
	    		else
		    		throw new FileNotFoundException("Unable to load propertiess file: " +propFileName) ;	    			
	    	} catch (IOException ex) {
	    		throw new FileNotFoundException("Unable to load propertiess file: "+propFileName) ;
	    	}
			return props;
		}
		
		public static void saveProp(Properties props,String fileName)
				throws IOException {
			FileWriter fw = new FileWriter(fileName);
			props.store(fw, null);
			fw.flush();
			fw.close();
		}

		/*
		public synchronized static boolean configureJdk14Logger(Level newLevel)
		{	
			try
			{
				//get the top Logger:
			    Logger topLogger = java.util.logging.Logger.getLogger("");
	
			    // Handler for console (reuse it if it already exists)
			    Handler consoleHandler = null;
			    //see if there is already a console handler
			    for (Handler handler : topLogger.getHandlers()) {
			        if (handler instanceof ConsoleHandler) {
			            //found the console handler
			            consoleHandler = handler;
			            handler.setLevel(newLevel);
			            Formatter newFormatter = null;
						handler.setFormatter(newFormatter);
			        }
			    }
	
	
			    if (consoleHandler == null) {
			        //there was no console handler found, create a new one
			        consoleHandler = new ConsoleHandler();
			        topLogger.addHandler(consoleHandler);
			    }
			    //set the console handler to SEVERE:
			    consoleHandler.setLevel(newLevel);
				return true;		
			}catch(Throwable t)
			{
				t.printStackTrace();
				return false;
			}
		}
		 */

		public  static Object toJavaDataType(Object value,String classCanonicalName) throws DataConversionException
		{
			if(value==null)
				return value;
						
			if((value instanceof byte[]) && (classCanonicalName.startsWith("java.lang.Byte")))
				return value;
									
			if(value.getClass().getCanonicalName().equals(classCanonicalName))
				return value;
			
			if(value instanceof InputStream)
			{
				try {
					value = toBytes((InputStream)value);
				} catch (IOException e) {
					e.printStackTrace();
					throw new DataConversionException(e.toString());
				}
			}else if(value instanceof Reader)
			{
				value = toString((Reader) value);
			}
			if(value instanceof UUID)
			{
				value = ((UUID)value).toString();
			}else 
			if(value instanceof RowId)
			{
				value = ((RowId)value).toString();		
			}

			if(value==null)
				return value;
			

			switch (classCanonicalName) {			
			case "B[":
				if(value instanceof String)
				{
					return value.toString().getBytes();					
				}else if(MethodUtils.getMatchingAccessibleMethod(value.getClass(), "toBytes", new Class[0]) != null)
				{
					try {
						return MethodUtils.invokeMethod(value, "toBytes", new Class[0]);
					} catch (Throwable t) {
						t.printStackTrace();
						throw new DataConversionException(t.toString());
					}					
				}else if(MethodUtils.getMatchingAccessibleMethod(value.getClass(), "getBytes", new Class[0]) != null)
				{
					try {
						return MethodUtils.invokeMethod(value, "getBytes", new Class[0]);
					} catch (Throwable t) {
						t.printStackTrace();
						throw new DataConversionException(t.toString());
					}					
				}else if(value instanceof Serializable)
				{
					try {
						return serialize(value);
					} catch (IOException e) {
						e.printStackTrace();
						throw new DataConversionException(e.toString());
					}
				}else
				{
					return value.toString().getBytes();
				}
			case "java.lang.String":
				if(value instanceof String)
				{
					return value.toString();
				}else if(value instanceof byte[])
				{
					return Base64.encodeBase64String((byte[])value);
				}else if(MethodUtils.getMatchingAccessibleMethod(value.getClass(), "stringValue", new Class[0]) != null)
				{
					try {
						return MethodUtils.invokeExactMethod(value, "stringValue", null,null);
					} catch (Throwable t) {
						t.printStackTrace();
						throw new DataConversionException(t.toString());
					}
				}else
				{
						String s = value.toString();
						if(s.endsWith(Integer.toHexString(value.hashCode())))
						{
							//TODO Find other methods to get string from object
							if(value instanceof Serializable)
							{
								try {
									return Base64.encodeBase64String(serialize(value));
								} catch (IOException e) {
									e.printStackTrace();
									throw new DataConversionException(e.toString());
								}
							}
						}
					return s;
				}
			case "java.util.Date":
				if (value instanceof String) {
					try {
						return java.sql.Timestamp.valueOf(value.toString());
					} catch (Throwable t) {
						// We are assuming the date is in UTC if date string does
						// not contain a timezone in this format
						// 2012-05-15T20:31:03.000Z
						try {
							DatatypeFactory df = DatatypeFactory.newInstance();
							XMLGregorianCalendar gc = df
									.newXMLGregorianCalendar(value.toString());
							if (gc.getTimezone() == javax.xml.datatype.DatatypeConstants.FIELD_UNDEFINED)
								gc.setTimezone(0); // UTC
							return new java.sql.Timestamp(gc.toGregorianCalendar(
									null, null, null).getTimeInMillis());
						} catch (Throwable e) {
							t.printStackTrace();
							throw new DataConversionException("Uparsable timestamp:"
									+ value.toString()+": "+e.toString());
						}
					}
				}
				return ConvertUtils.convert(value, java.sql.Timestamp.class);
			case "java.lang.Boolean":
				 String boolvalue = value.toString();
				if(boolvalue.equalsIgnoreCase(Boolean.TRUE.toString()) || boolvalue.equalsIgnoreCase("YES") || boolvalue.equalsIgnoreCase("1") || boolvalue.equalsIgnoreCase("Y"))
					return Boolean.TRUE;
				else
					return ConvertUtils.convert(value, Boolean.class);
			case "java.math.BigDecimal":
				if(value instanceof String)
					return new BigDecimal(value.toString());
				else
					return ConvertUtils.convert(value, BigDecimal.class);
			default:
				if(value instanceof String)
					value = new BigDecimal(value.toString());
				try {
					return ConvertUtils.convert(value, classForJavaDataTypeFullClassName(classCanonicalName));
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					throw new DataConversionException(e.toString());
				}
			}	
		}


		public static String toString(Reader input) {
			if(input == null)
				return null;
			try {
				StringBuffer sbuf = new StringBuffer();
				char[] cbuf = new char[DEFAULT_BUFFER_SIZE];
				int count = -1;
				try {
					int n;
					while ((n = input.read(cbuf)) != -1)
					{
						sbuf.append(cbuf, 0, n);
						count = ((count == -1) ? n : (count + n));
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (count == -1)
					return null;
				else
					return sbuf.toString();
			} finally {
				if (input != null)
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}
		
		public static byte[] toBytes(InputStream input) throws IOException {
			if(input == null)
				return null;
			try 
			{
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
			int n = 0;
			int count = -1;
			while (EOF != (n = input.read(buffer))) {
				output.write(buffer, 0, n);
				count = ((count == -1) ? n : (count + n));
			}
			output.flush();
			if(count == -1)
				return null;
			else
				return output.toByteArray();
			} finally {
				if (input != null)
					try {
						input.close();
					} catch (IOException e) {e.printStackTrace();}
			}
		} 
		
	    public static byte[] serialize(Object obj) throws IOException {
	        ByteArrayOutputStream b = new ByteArrayOutputStream();
	        ObjectOutputStream o = new ObjectOutputStream(b);
	        o.writeObject(obj);
	        return b.toByteArray();
	    }
	    
	    
		public static String replaceString(String original, String pattern, String replace) 
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

		public static String getCSVFriendlyString(String content)
		{
			if(content!=null && !content.isEmpty())
			{
			content = ConnectorUtils.replaceString(content, "" + COMMA, "");
			content = ConnectorUtils.replaceString(content, "" + CR, "");
			content = ConnectorUtils.replaceString(content, "" + LF, "");
			content = ConnectorUtils.replaceString(content, "" + QUOTE, "");
			}
			return content;
		}
		
		
	public static Class<?> classForJavaDataTypeFullClassName(String cannonicalName) throws ClassNotFoundException 
	{
		if (cannonicalName.equals(byte[].class.getCanonicalName()))
			cannonicalName = byte[].class.getName();
		return Class.forName(cannonicalName);
	}
	
	
public static void addDirectoryToClasspath(File dir, URLClassLoader classLoader) 
	{					
		try 
		{
			if(dir == null || !dir.exists() || !dir.isDirectory())
				throw new IllegalArgumentException("Directory {"+dir+"} not found");					

			if(classLoader == null)
				throw new IllegalArgumentException("Input classLoader is null");					

				Class<URLClassLoader> clazz = URLClassLoader.class;
				Method method = clazz.getDeclaredMethod("addURL", new Class[] { URL.class });
				method.setAccessible(true);
	
				File[] fileList = dir.listFiles();
				// List<URL> urlList = new ArrayList<URL>();
				method.invoke(classLoader, new Object[] { dir.toURI().toURL() });
	
				// urlList.add(dir.toURI().toURL());
				for (File f : fileList) {
					if (!f.isFile())
						continue;
					// urlList.add(f.toURI().toURL());
					method.invoke(classLoader,
							new Object[] { f.toURI().toURL() });
				}
					// URL[] urlArray = urlList.toArray(new URL[0]);				
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			}
	}
	
		
public static Map<String,String> getConnectorList()
{					
	LinkedHashMap<String,String> connectorMap = new LinkedHashMap<String,String>();
	List<String> connectorList = new FastClasspathScanner()
//	Set<String> connectorList = new FastClasspathScanner("com.sforce")
//	.verbose()
    .scan()
//    .getNamesOfAllClasses();
	.getNamesOfClassesImplementing(IConnector.class);
	for(String connectorClass: connectorList)
	{
		Object obj;
		try {
			obj = ConstructorUtils.invokeConstructor(Class.forName(connectorClass), new Object[0]);
			connectorMap.put(BeanUtils.getSimpleProperty(obj, "connectorID"), BeanUtils.getSimpleProperty(obj, "connectorName"));
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	return connectorMap;
}

public static IConnector getConnector(String connectorID)
{					
	List<String> connectorList = new FastClasspathScanner()
    .scan()
	.getNamesOfClassesImplementing(IConnector.class);
	for(String connectorClass: connectorList)
	{
		Object obj;
		try {
			obj = ConstructorUtils.invokeConstructor(Class.forName(connectorClass), new Object[0]);
			String id = BeanUtils.getSimpleProperty(obj, "connectorID");
			if(id != null && id.equals(connectorID))
			{
				return (IConnector) obj;
			}
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	throw new IllegalArgumentException("connectorID {"+connectorID+"} not found in classpath");
}


public static List<ConnectionProperty> getConnectionPropertyList(String connectorID)
{
	return ConnectionPropertyUtil.getConnectionProperties(getConnector(connectorID));
}

}
