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
package com.sforce.dataset.flow;

import java.util.Map;


public class DataFlow {
	
	public String _type = null;
	public UserType _lastModifiedBy;
	public String nextRun;
	public long nextRunTime = 0L;
	public String _url;
	public String name;
	public String MasterLabel;
	public int RefreshFrequencySec = 0;
	public String _uid;
	public String WorkflowType;
    @SuppressWarnings("rawtypes")
	public Map workflowDefinition;

	@Override
	public String toString() {
		return "DataFlow [_type=" + _type + ", _lastModifiedBy="
				+ _lastModifiedBy + ", nextRun=" + nextRun + ", nextRunTime="
				+ nextRunTime + ", _url=" + _url + ", name=" + name
				+ ", MasterLabel=" + MasterLabel + ", RefreshFrequencySec="
				+ RefreshFrequencySec + ", _uid=" + _uid + ", WorkflowType="
				+ WorkflowType + ", workflowDefinition=" + workflowDefinition
				+ "]";
	}

	class UserType {
        public String _type = null;
        public String profilePhotoUrl = null;
        public String name = null;
        public String _uid = null;
		@Override
		public String toString() {
			return "UserType [_type=" + _type + ", profilePhotoUrl="
					+ profilePhotoUrl + ", name=" + name + ", _uid=" + _uid
					+ "]";
		}
    }
    
	public static UserType getUserType(DataFlow obj,Map<String,String> input)
	{
		if(input!=null && !input.isEmpty())
		{
			UserType ret =  obj.new UserType();
			ret._type = (String) input.get("_type");
			ret._uid = (String) input.get("_uid");
			ret.name = (String) input.get("name");
			ret.profilePhotoUrl = (String) input.get("profilePhotoUrl");
			return ret;
		}
		return null;
	}

}
