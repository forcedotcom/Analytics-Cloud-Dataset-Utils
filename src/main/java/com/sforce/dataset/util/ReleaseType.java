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

