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
package com.sforce.dataset.connector.metadata;

import java.util.HashMap;

public class FieldType {	
	
	private String name = null; //Required
	private String fullyQualifiedName = null; //Required
	private String label = null; //Required
	private String description = null; //Optional
	private Class<?> type = null; //Required - Text, Numeric, Date
	private int precision = 0; //Required if type is Numeric, the number 256.99 has a precision of 5
	private int scale = 0; //Required if type is Numeric, the number 256.99 has a scale of 2
	private String defaultValue = null; //required for numeric types	
	private String format = null; //Required if type is Numeric or Date
	private boolean systemField = false; //Optional
	private boolean uniqueId = false; //Optional
	private boolean multiValue = false; //Optional 
	private String multiValueSeparator = null; //Optional - only used if IsMultiValue = true separator
	private int fiscalMonthOffset = 0;
	private int firstDayOfWeek = -1; //1=SUNDAY, 2=MONDAY etc.. -1 the week starts on 1st day of year and is always 7 days long
	private boolean canTruncateValue = true; //Optional 
	private boolean yearEndFiscalYear = true; //Optional 
	private boolean skipped = false; //Optional
	private boolean nillable = false;
	private boolean filterable = false;
	private HashMap<String, String> extension=null;
	//toDO RELATEDoBJECTS
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFullyQualifiedName() {
		return fullyQualifiedName;
	}
	public void setFullyQualifiedName(String fullyQualifiedName) {
		this.fullyQualifiedName = fullyQualifiedName;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public Class<?> getType() {
		return type;
	}
	public void setType(Class<?> type) {
		this.type = type;
	}
	public int getPrecision() {
		return precision;
	}
	public void setPrecision(int precision) {
		this.precision = precision;
	}
	public int getScale() {
		return scale;
	}
	public void setScale(int scale) {
		this.scale = scale;
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public boolean isSystemField() {
		return systemField;
	}
	public void setSystemField(boolean systemField) {
		this.systemField = systemField;
	}
	public boolean isUniqueId() {
		return uniqueId;
	}
	public void setUniqueId(boolean uniqueId) {
		this.uniqueId = uniqueId;
	}
	public boolean isMultiValue() {
		return multiValue;
	}
	public void setMultiValue(boolean multiValue) {
		this.multiValue = multiValue;
	}
	public String getMultiValueSeparator() {
		return multiValueSeparator;
	}
	public void setMultiValueSeparator(String multiValueSeparator) {
		this.multiValueSeparator = multiValueSeparator;
	}
	public int getFiscalMonthOffset() {
		return fiscalMonthOffset;
	}
	public void setFiscalMonthOffset(int fiscalMonthOffset) {
		this.fiscalMonthOffset = fiscalMonthOffset;
	}
	public int getFirstDayOfWeek() {
		return firstDayOfWeek;
	}
	public void setFirstDayOfWeek(int firstDayOfWeek) {
		this.firstDayOfWeek = firstDayOfWeek;
	}
	public boolean isCanTruncateValue() {
		return canTruncateValue;
	}
	public void setCanTruncateValue(boolean canTruncateValue) {
		this.canTruncateValue = canTruncateValue;
	}
	public boolean isYearEndFiscalYear() {
		return yearEndFiscalYear;
	}
	public void setYearEndFiscalYear(boolean yearEndFiscalYear) {
		this.yearEndFiscalYear = yearEndFiscalYear;
	}
	public boolean isSkipped() {
		return skipped;
	}
	public void setSkipped(boolean skipped) {
		this.skipped = skipped;
	}
	public boolean isNillable() {
		return nillable;
	}
	public void setNillable(boolean nillable) {
		this.nillable = nillable;
	}
	public boolean isFilterable() {
		return filterable;
	}
	public void setFilterable(boolean filterable) {
		this.filterable = filterable;
	}
	public HashMap<String, String> getExtension() {
		return extension;
	}
	public void setExtension(HashMap<String, String> extension) {
		this.extension = extension;
	}
	
	
	
}
