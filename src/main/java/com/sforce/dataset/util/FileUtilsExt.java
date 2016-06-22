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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;


public class FileUtilsExt {

	public static final Pattern p = Pattern.compile("copy\\d+$");
	
	public static File getUniqueFile(File file)
	{
		if(file==null)
			return file;
		
		if(file.exists())
		{			
			int ver = -1;
			while(file.exists())
			{
				String name = FilenameUtils.getBaseName(file.getName());
				if(ver==-1)
				{
					ver = 0;
					String version = null;
					if(name != null)
					{
						Matcher m = p.matcher(name);
						if(m.find())
						{
							version = m.group();
						}
						if(version!=null && !version.trim().isEmpty())
						{
							BigDecimal bd = new BigDecimal(version.replace("copy", ""));
							if(bd!=null)
							{	
								ver = bd.intValue();
								if(ver > 99)
								{
									ver = 0;
								}				
							}
							if(name.endsWith(version))
							{
								name  = name.replace(version, "");
							}
						}
					}else
					{
						name = "";
					}
				}
				if(name.endsWith("copy"+ver))
				{
					name  = name.replace("copy"+ver, "");
				}
				ver++;
				if(FilenameUtils.getExtension(file.getName())==null || FilenameUtils.getExtension(file.getName()).trim().isEmpty())
				{
					name = name + "copy" + ver;
				}else
				{
					name = name + "copy" + ver + "." + FilenameUtils.getExtension(file.getName());
				}
				if(file.getAbsoluteFile().getParentFile()==null)
				{
					file = new File(name);
				}else
				{
					file = new File(file.getAbsoluteFile().getParentFile(),name);
				}
			}
		}
		return file;
	}
	
	public static boolean deleteQuietly(File file)
	{
		if(file != null)
		{
			Path path = Paths.get(file.getAbsolutePath());
			try {
				return Files.deleteIfExists(path);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
}
