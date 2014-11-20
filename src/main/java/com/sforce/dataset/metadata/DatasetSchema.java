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
package com.sforce.dataset.metadata;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DatasetSchema {

	private static final String filename = "main.json";
	
	public static void save(File schemaFile,DatasetSchema emd)
	{
		ObjectMapper mapper = new ObjectMapper();	
		try 
		{
			mapper.writerWithDefaultPrettyPrinter().writeValue(schemaFile, emd);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static Map load(File emDir) throws JsonParseException, JsonMappingException, IOException
	{
		File schemaFile = getSchemaFile(emDir);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Map userSchema = null;
		if(schemaFile.exists())
		{
//			System.out.println("Loading em schema from file {"+ schemaFile +"}");
			userSchema  =  mapper.readValue(schemaFile,Map.class);			
		}
		return userSchema;
	}

	public static File getSchemaFile(File emDir) throws IOException {
				emDir = new File(emDir,filename);
				if(emDir.exists())
					return emDir;
				else
					throw new IOException("File {"+emDir+"} not found");
	}
	

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Map<String,Map> getDimensions(File emDir) throws JsonParseException, JsonMappingException, IOException
	{
		Map temp = load(emDir);
		if(temp!=null)
		{
			Map<String,Map> dims = (Map<String,Map>) temp.get("dimensions");
			if(dims!=null)
			{
				return dims;
			}
		}
		throw new IOException("No dimensions found in {"+getSchemaFile(emDir)+"} ");
	}

	
}
