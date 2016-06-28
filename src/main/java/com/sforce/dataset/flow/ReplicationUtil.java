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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class ReplicationUtil {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static DataFlow getReplicationDefinitionFromUserDataflow(PartnerConnection partnerConnection) throws ConnectionException, URISyntaxException, ClientProtocolException, IOException
	{
		DataFlow df = null;
		LinkedHashMap replicationFlow = new LinkedHashMap();
		List<DataFlow> flows = DataFlowUtil.listDataFlow(partnerConnection);

		for(DataFlow flow:flows)
		{
			if(flow.getWorkflowType().equalsIgnoreCase("SfdcExtract"))
			{
				df = DataFlowUtil.getDataFlow(partnerConnection, flow.getName(), flow.get_uid());
				Map def = df.getWorkflowDefinition();
				for(Object key:def.keySet())
				{
					Object val = def.get(key);
					if(val instanceof Map)
					{
						String object = null;
						Map node = (Map)val;
						String action = (String) node.get("action");
						if(action != null && action.equalsIgnoreCase("sfdcDigest"))
						{
							Object params = node.get("parameters");
							if(params instanceof Map)
							{
								object = (String) ((Map)params).get("object");
								if(object==null)
									continue;
							}else
							{
								continue;
							}
							
							replicationFlow.put(object, val);
						}
					}
				}
				break; //There can be only one replication dataflow
			}
		}

		for(DataFlow flow:flows)
		{
			if(!flow.getWorkflowType().equalsIgnoreCase("User") || !flow.getStatus().equalsIgnoreCase("Active") )
				continue;
			
			DataFlow userWf = DataFlowUtil.getDataFlow(partnerConnection, flow.getName(), flow.get_uid());
			Map def = userWf.getWorkflowDefinition();
			for(Object key:def.keySet())
			{
				Object val = def.get(key);
				if(val instanceof Map)
				{
					String object = null;
					Map node = (Map)val;
					String action = (String) node.get("action");
					if(action != null && action.equalsIgnoreCase("sfdcDigest"))
					{
						Object params = node.get("parameters");
						if(params instanceof Map)
						{
							object = (String) ((Map)params).get("object");
							if(object==null)
								continue;
						}else
						{
							continue;
						}
						
						if(replicationFlow.containsKey(object))
						{
							val = mergeNode((Map)replicationFlow.get(object),node, object);
						}
						replicationFlow.put(object, val);
					}
				}
			}
		}
		if(df!=null)
		{
			df.setWorkflowDefinition(replicationFlow);
		}
		return df;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Map mergeNode(Map first, Map second, String Object) {
		List<Map<String,String>> flds = new LinkedList<Map<String,String>>();
		List<Map<String,String>> firstList = (List<Map<String, String>>) first.get("fields");
		List<Map<String,String>> secondList = (List<Map<String, String>>) second.get("fields");
		if(firstList != null && secondList != null)
		{
			for(Map<String, String> fld1:firstList)
			{
				String name1 = fld1.get("name");
				if(name1 != null && !name1.isEmpty())
				{
					for(Map<String, String> fld2:secondList)
					{
						String name2 = fld2.get("name");
						if(name2 != null && !name2.isEmpty())
						{
							if(name1.equalsIgnoreCase(name2))
							{
								//fld1.putAll(fld2);
								fld2.putAll(fld1);
								fld1 = fld2;
							}
						}
					}
					flds.add(fld1);
				}
			}
			
			for(Map<String, String> fld2:secondList)
			{
				String name2 = fld2.get("name");
				if(name2 != null && !name2.isEmpty())
				{
					boolean found = false;
					for(Map<String, String> fld1:firstList)
					{
						String name1 = fld1.get("name");
						if(name1 != null && !name1.isEmpty())
						{
							if(name2.equalsIgnoreCase(name1))
							{
								found=true;
								break;
							}
						}
					}
					if(!found)
					flds.add(fld2);
				}
			}
			first.remove("fields");
			first.put("fields", flds);
			
			if(first.containsKey("complexFilterConditions"))
			{
				if(second.containsKey("complexFilterConditions"))
				{
					if(!first.get("complexFilterConditions").equals(second.get("complexFilterConditions")))
					{
						System.out.println("Filter mismatch for object {"+Object+"}, Please merge filter manually");
						//first.remove("complexFilterConditions");
					}
				}else
				{
					System.out.println("Filter mismatch for object {"+Object+"}, Please merge filter manually");
					//first.remove("complexFilterConditions");
				}
			}else
			{
				if(second.containsKey("complexFilterConditions"))
				{
					System.out.println("Filter mismatch for object {"+Object+"}, Please merge filter manually");
				}
			}

			if(first.containsKey("filterConditions"))
			{
				if(second.containsKey("filterConditions"))
				{
						System.out.println("Filter mismatch for object {"+Object+"}, Please merge filter manually");
				}else
				{
						System.out.println("Filter mismatch for object {"+Object+"}, Please merge filter manually");
				}
			}else
			{
				if(second.containsKey("filterConditions"))
				{
						System.out.println("Filter mismatch for object {"+Object+"}, Please merge filter manually");
				}
			}
			
			if(second.containsKey("schema"))
			{
					System.out.println("Please manually merge schema for object {"+Object+"}");
			}
			
		}
		return first;
	}

}
