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
package com.sforce.dataset.loader.file.sort;

import com.foundations.comparator.attributes.DateTimeSortAttributes;
import com.foundations.comparator.attributes.DecimalSortAttributes;
import com.foundations.comparator.attributes.SortAttributes;
import com.foundations.comparator.attributes.StringSortAttributes;
import com.foundations.comparator.column.DateTimeComparator;
import com.foundations.comparator.column.DecimalComparator;
import com.foundations.comparator.column.IColumnComparator;
import com.foundations.comparator.column.IntegerComparator;
import com.foundations.comparator.column.StringComparator;
import com.sforce.dataset.loader.file.schema.FieldType;

public class CsvColumnComparatorFactory {
	
	
	public static final IColumnComparator createColumnComparator(FieldType fieldType) {
		IColumnComparator comparator = null;
		if(fieldType.getfType() ==  FieldType.STRING) {
			StringSortAttributes attributes = new StringSortAttributes();
			attributes.setCaseSensitive(true);
			attributes.setAscendingOrder(fieldType.isSortAscending);
			comparator = new StringComparator(fieldType.getName(), fieldType.getSortIndex(), attributes);
		}
		else if(fieldType.getfType() == FieldType.DATE) {
			DateTimeSortAttributes attributes = new DateTimeSortAttributes();
			attributes.setAscendingOrder(fieldType.isSortAscending);
			attributes.setPattern(fieldType.getFormat());
			comparator = new DateTimeComparator(fieldType.getName(), fieldType.getSortIndex(), attributes);				
		}
		else if(fieldType.getfType() == FieldType.MEASURE)
		{
			if(fieldType.getScale() > 0) 
			{
				DecimalSortAttributes attributes = new DecimalSortAttributes();
				attributes.setAscendingOrder(fieldType.isSortAscending);
				attributes.setScale(fieldType.getScale());
				comparator = new DecimalComparator(fieldType.getName(), fieldType.getSortIndex(), attributes);
			}else
			{
				SortAttributes attributes = new SortAttributes();
				attributes.setAscendingOrder(fieldType.isSortAscending);
				comparator = new IntegerComparator(fieldType.getName(), fieldType.getSortIndex(), attributes);
			}
			
		}
		return comparator;
	}		
}
