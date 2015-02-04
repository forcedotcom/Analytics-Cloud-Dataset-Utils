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
package com.sforce.dataset.loader.file.schema;


public class FieldType {
	public String name = null; //Required
	public String fullyQualifiedName = null; //Required
	public String label = null; //Required
	public String description = null; //Optional
	public String type = null; //Required - Text, Numeric, Date
	public int precision = 0; //Required if type is Numeric, the number 256.99 has a precision of 5
	public int scale = 0; //Required if type is Numeric, the number 256.99 has a scale of 2
	public String decimalSeparator = ".";
	public String defaultValue = null; //required for numeric types	
	public String format = null; //Required if type is Date
	public boolean isSystemField = false; //Optional
	public boolean isUniqueId = false; //Optional
	public boolean isMultiValue = false; //Optional 
	public String multiValueSeparator = null; //Optional - only used if IsMultiValue = true separator
	public int fiscalMonthOffset = 0;
	public int firstDayOfWeek = -1; //1=SUNDAY, 2=MONDAY etc.. -1 the week starts on 1st day of year and is always 7 days long
	public boolean isYearEndFiscalYear = true; //Optional 
	public boolean canTruncateValue = true; //Optional 
	public boolean isSkipped = false; //Optional 
}
