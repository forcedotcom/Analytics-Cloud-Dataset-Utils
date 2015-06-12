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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ReleaseType {
	public String url = null;
	public long id = 0L;
	public String name = null;
	public String assets_url = null;
	public String upload_url = null;
	public String html_url = null;
	public String tag_name = null;
	public String target_commitish = null;
	public boolean draft = false;
	public boolean prerelease = false;
	public String  created_at = null;
	public String  published_at = null;
	public Map<String,?> author = null;
	public List<Map<String,?>> assets = null;
	
	class AssetsType 
	{
		public String url = null;
		public long id = 0L;
		public String name = null;
		public String label = null;
		public String content_type = null;
		public String state = null;
		public long size = 0L;
		public long download_count = 0L;
		public String  created_at = null;
		public String  published_at = null;
		public String browser_download_url;
	}
	
	public List<AssetsType> getAssetList()
	{
		if(assets!=null)
		{
			List<AssetsType> list = new LinkedList<AssetsType>();
			for(Map<String, ?> asset:assets)
			{
				AssetsType temp = new AssetsType();
				temp.browser_download_url = (String) asset.get("browser_download_url");
				temp.name = (String) asset.get("name");
				temp.download_count = ((Number) asset.get("download_count")).longValue();
				temp.size = ((Number) asset.get("size")).longValue();
				list.add(temp);
			}
			return list;
		}
		return null;		
	}
}

