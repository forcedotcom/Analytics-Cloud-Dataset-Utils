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

import java.util.LinkedHashMap;
import java.util.Map;

public class DatasetType implements Comparable<DatasetType> {
    public String assetIcon = null;
    public String _uid = null;
    public String _type = null;
    public long _createdDateTime = 0L;
    public long _lastAccessed = 0L;
    public String name = null;
    public String _alias = null;
    public String _url = null;
    public String assetIconUrl = null;
    public String assetSharingUrl = null;
    public FolderType folder = null;
    public UserType _createdBy = null;
    public PermissionType _permissions = null;
    public EdgemartDataType edgemartData = null;
    public Map<String,String> _files = null; 
    class FolderType {
        public String _uid = null;
        public String _type = null;
        public String name = null;
        public String label = null;
		@Override
		public String toString() {
			return "FolderType [_uid=" + _uid + ", _type=" + _type + ", name="
					+ name + ", label=" + label + "]";
		}
    }
    class EdgemartDataType {
        public String _uid = null;
        public String _type = null;
        public long _createdDateTime = 0L;
        public UserType _createdBy = null;
		@Override
		public String toString() {
			return "EdgemartDataType [_uid=" + _uid + ", _type=" + _type
					+ ", _createdDateTime=" + _createdDateTime
					+ ", _createdBy=" + _createdBy + "]";
		}
    }
    class PermissionType {
    	public boolean modify=false;
    	public boolean manage=false;
    	public boolean view=false;
		@Override
		public String toString() {
			return "PermissionType [modify=" + modify + ", manage=" + manage
					+ ", view=" + view + "]";
		} 
    }
    class UserType {
     public String _type = null;
     public String profilePhotoUrl = null;
     public String name = null;
     public String _uid = null;
	@Override
	public String toString() {
		return "UserType [_type=" + _type + ", profilePhotoUrl="
				+ profilePhotoUrl + ", name=" + name + ", _uid=" + _uid + "]";
	}
    }
    
	
	@Override
	public String toString() {
		return "DatasetType [assetIcon=" + assetIcon + ", _uid=" + _uid
				+ ", _type=" + _type + ", _createdDateTime=" + _createdDateTime
				+ ", _lastAccessed=" + _lastAccessed + ", name=" + name
				+ ", _alias=" + _alias + ", _url=" + _url + ", assetIconUrl="
				+ assetIconUrl + ", assetSharingUrl=" + assetSharingUrl
				+ ", folder=" + folder + ", _createdBy=" + _createdBy
				+ ", _permissions=" + _permissions + ", edgemartData="
				+ edgemartData + ", _files=" + _files + "]";
	}
	
	
	@Override
	public int compareTo(DatasetType o) {
//		if(this.edgemartData!=null && o.edgemartData!=null)
//		{
//			if (this.edgemartData._createdDateTime > o.edgemartData._createdDateTime)
//				return 1;
//			else if (this.edgemartData._createdDateTime < o.edgemartData._createdDateTime)
//				return -1;
//			else
//				return 0;
//		}
		
		if (this._createdDateTime > o._createdDateTime)
			return 1;
		else if (this._createdDateTime < o._createdDateTime)
			return -1;
		else
			return 0;

	}
	

	@SuppressWarnings("unchecked")
	public static DatasetType getDatasetType(Map<String,?> input)
	{
		DatasetType ret = null;
		if(input!=null && !input.isEmpty())
		{
			
			String _type = (String) input.get("_type");
			if(_type == null || !_type.equals("edgemart"))
			{
				return ret;
			}

			ret = new DatasetType();
			
			Object temp = input.get("_createdDateTime");
			if(temp != null && temp instanceof Number)
			{
				ret._createdDateTime = ((Number)temp).longValue()*1000L;
			}

			temp = input.get("_lastAccessed");
			if(temp != null && temp instanceof Number)
			{
				ret._lastAccessed = ((Number)temp).longValue()*1000L;
			}

			ret._type  = (String) input.get("_type");
			
			ret._uid  = (String) input.get("_uid");
			
			ret.assetIcon = (String) input.get("_type");
			
			ret.assetIconUrl = (String) input.get("assetIconUrl");
			
			ret.assetSharingUrl = (String) input.get("assetSharingUrl");
			
			ret._alias = (String) input.get("_alias");

			ret.name = (String) input.get("name");
			
			ret._url = (String) input.get("_url");
			
			ret.edgemartData = DatasetType.getEdgemartDataType(ret, (Map<String, ?>) input.get("edgemartData"));
			ret.folder = DatasetType.getFolderType(ret, (Map<String, String>) input.get("folder"));
			ret._createdBy = DatasetType.getUserType(ret, (Map<String, String>) input.get("_createdBy"));
			ret._permissions = DatasetType.getPermissionType(ret, (Map<String, ?>) input.get("_permissions"));
			ret._files = DatasetType.getJsonFiles((Map<String, String>) input.get("_files"));
		}
		return ret;
	}

	public static Map<String,String> getJsonFiles(Map<String,String> input)
	{
		if(input!=null && !input.isEmpty())
		{
			LinkedHashMap<String,String> files = new LinkedHashMap<String,String>();
			for(String file:input.keySet())
			{
				if(file.toLowerCase().endsWith("json"))
				{
					files.put(file, input.get(file));
				}
			}
			return files;
		}
		return null;
	}
	
	public static PermissionType getPermissionType(DatasetType obj,Map<String,?> input)
	{
		PermissionType ret = obj.new PermissionType();
		if(input!=null && !input.isEmpty())
		{
			Object var = input.get("modify");
			if(var != null && var instanceof Boolean)
			{
				ret.modify = ((Boolean)var).booleanValue();
			}

			var = input.get("manage");
			if(var != null && var instanceof Boolean)
			{
				ret.manage = ((Boolean)var).booleanValue();
			}

			var = input.get("view");
			if(var != null && var instanceof Boolean)
			{
				ret.view = ((Boolean)var).booleanValue();
			}
		}
		return ret;
	}

	public static FolderType getFolderType(DatasetType obj,Map<String,?> input)
	{
		FolderType ret = obj.new FolderType();
		if(input!=null && !input.isEmpty())
		{
			ret._uid = (String) input.get("_uid");
			ret._type = (String) input.get("_type");
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public static EdgemartDataType getEdgemartDataType(DatasetType obj,Map<String,?> input)
	{
		EdgemartDataType ret = obj.new EdgemartDataType();
		if(input!=null && !input.isEmpty())
		{
			ret._uid = (String) input.get("_uid");
			Object temp = input.get("_createdDateTime");
			if(temp != null && temp instanceof Number)
			{
				ret._createdDateTime = ((Number)temp).longValue()*1000L;
			}			
			ret._type = (String) input.get("_type");
			ret._createdBy = DatasetType.getUserType(obj, (Map<String, String>) input.get("_createdBy"));
		}
		return ret;
	}

	public static UserType getUserType(DatasetType obj,Map<String,String> input)
	{
		UserType ret =  obj.new UserType();
		if(input!=null && !input.isEmpty())
		{
			ret._type = (String) input.get("_type");
			ret._uid = (String) input.get("_uid");
			ret.name = (String) input.get("name");
			ret.profilePhotoUrl = (String) input.get("profilePhotoUrl");
		}
		return ret;
	}

    
    
}