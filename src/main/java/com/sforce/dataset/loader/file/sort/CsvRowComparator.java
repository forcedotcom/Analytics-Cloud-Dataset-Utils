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

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.foundations.comparator.column.IColumnComparator;
import com.sforce.dataset.loader.file.schema.ext.FieldType;

public class CsvRowComparator implements Comparator<List<String>> {

	private IColumnComparator[] _columnComparators;
	private int[] _sortColumnIndices;
	int maxSortIndex = 0;

	public CsvRowComparator(List<FieldType> fields) {
		_columnComparators = getColumnComparators(fields);
		setSortColumnIndices(fields);
	}

	private IColumnComparator[] getColumnComparators(List<FieldType> fields) 
	{
		List<IColumnComparator> list = new LinkedList<IColumnComparator>();
		for(FieldType fld:fields) {
			if(!fld.isComputedField && fld.getSortIndex() > 0) {
				list.add(CsvColumnComparatorFactory.createColumnComparator(fld));
			}
		}
		_columnComparators = list.toArray(new IColumnComparator[list.size()]);		
		return _columnComparators;
	}

	/**
	 * Loops through each ColumnComparator to determine the proper sort order sequence
	 * to execute during the sort. Called by constructor.
	 * 
	 * - ensures the sort order is unique when non-zero
	 * - ensures sort order values of zero are ignored
	 * - ensures the sort order starts at 1
	 * - ensures the sort order contain no gaps, such as 1, 2, 4
	 * @param fields 
	 * 
	 */
	private void setSortColumnIndices(List<FieldType> fields) {
		int size = _columnComparators.length;
		boolean foundIndex=false;
		_sortColumnIndices = new int[size];
		for( int sortOrder = 1; sortOrder <= size; sortOrder++ ) {
			foundIndex = false;
			for( int comparatorIndex = 0; comparatorIndex < fields.size(); comparatorIndex++ ) {
				if( fields.get(comparatorIndex).getSortIndex() == sortOrder ) {
					_sortColumnIndices[sortOrder - 1] = comparatorIndex;
					if(comparatorIndex > maxSortIndex)
						maxSortIndex = comparatorIndex;
					foundIndex = true;
					break;
				}
			}
			if( !foundIndex ) {
				throw new IllegalArgumentException("Illegal sortIndex defined in input metadata json, must start at 1, be unique and not contain gaps");
			}
		}
		if( !foundIndex && size > 0) {
			throw new IllegalArgumentException("Illegal sortIndex defined in input metadata json, must start at 1, be unique and not contain gaps");
		}
	}

	
	public IColumnComparator getColumnComparator(int index) {
		return _columnComparators[index];
	}
	
	/**
	 * Compares two rows returning the result of the compare.
	 * 
	 * - The columns to be sorted will have been pre-determined.
	 * - The choice of delimiter separating the row's columns has been pre-determined
	 * - The number of columns has been pre-determined
	 * - The column datatypes has been pre-determined
	 * 
	 * The first non-zero comparison is returned respecting sort order priority. A
	 * return value of zero indicates the rows are equal looking only at the columns
	 * deemed sortable (Those with a non-zero sort order value)  
	 * 
	 * @param a The first row to compare.
	 * @param b The second row to compare.
	 */
	public int compare(List<String> a, List<String> b) 
	{
		int result = 0;
		if( a.size() < maxSortIndex ) { 
			throw new IllegalArgumentException("Incorrect number of tokens detected:\n\n" + a + "\n");
		}

		if( b.size() < maxSortIndex ) {
			throw new IllegalArgumentException("Incorrect number of tokens detected:\n\n" + b + "\n");
		}

		int compartorCnt=0;
		for( int index : _sortColumnIndices ) {
			result = _columnComparators[compartorCnt].compare(a.get(index), b.get(index));
			if( result != 0 ) {
				break;
			}
			compartorCnt++;
		}
		return result;
	}
	
	/**
	 * Iterates through this TableComparator's array of IColumnComparator
	 * and returns the count of columns that are sortable. A sortable 
	 * column is one where its sort order is greater than 0
	 * 
	 * @return the number of sortable columns
	 */
	public int getSortColumnCount() {
		return _columnComparators.length;
	}
	
	

}
