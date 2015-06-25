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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.flow.node.FilterCondition;
import com.sforce.dataset.flow.node.FilterExpression;

public class testFilters {

	public static void main(String[] args) {
		
		FilterExpression exp = null;
		
//		BigDecimal bd = new BigDecimal(2);
//		bd.setScale(10, RoundingMode.HALF_EVEN);

		LinkedList<FilterExpression> subconditions = new LinkedList<FilterExpression>();

		exp = FilterExpression.getFilterExpression("AccountName", "contains", "SFDC", false, null);
		subconditions.add(exp);
		
		exp = FilterExpression.getFilterExpression("OwnerId", "=", "a07B00000012HYu", false, null);
		subconditions.add(exp);
		
		FilterCondition subcond = FilterCondition.getFilterCondition("OR", subconditions);

		LinkedList<FilterExpression> conditions = new LinkedList<FilterExpression>();
		
		exp = FilterExpression.getFilterExpression("IsClosed", "=", "true", false, subcond);
		conditions.add(exp);

		FilterCondition cond = FilterCondition.getFilterCondition("AND", conditions);
		
		try 
		{
				ObjectMapper mapper = new ObjectMapper();	
				String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cond);
				System.out.println(json);
				FilterCondition filterCondition = mapper.readValue(json, FilterCondition.class);		
				if(!filterCondition.equals(cond))
				{
					throw new Exception("Failed to searliaze and deserialize filter");
				}
		} catch (Throwable t) 
		{
				t.printStackTrace();
		}
		

	}

}
