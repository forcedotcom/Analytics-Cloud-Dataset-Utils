package com.foundations.comparator.column;

import com.foundations.comparator.attributes.SortAttributes;

public abstract class AbstractComparator implements IColumnComparator {
	private String _name;	
	private int _sortOrder;
	private SortAttributes _sortAttributes;

	public AbstractComparator(String name, int sortOrder, SortAttributes sortAttributes) {
		_name = name;
		_sortOrder = sortOrder;
		_sortAttributes = sortAttributes;
	}

	public String getName() {
		return _name;
	}

	public int getSortOrder() {
		return _sortOrder;
	}

	public SortAttributes getSortAttributes() {
		return _sortAttributes;
	}

	/**
	 * Returns a result of zero, greater than one, or less than one
	 * depending on the comparison of the two supplied Strings<p>
	 * 
	 * A return value of zero indicates the two Strings are equal
	 * A return value greater than one indicates String a is bigger than String b
	 * A return value less than one indicates String a is less than String b
	 * 
	 * The first step in comparing the Strings involves swapping them if they are not
	 * already in ascending order. 
	 * 
	 * Next, any required trimming is performed if the Trim attribute has been set.
	 * The Strings are then normalized, ensuring zero-length Strings are treated as 
	 * nulls.
	 * 
	 * If both Strings turn out to be null after normalization, zero is returned.
	 * If one of the two Strings is null, the compare will consider the NullLowSortOrder 
	 * attribute to determine the final result.
	 * 
	 * If both Strings are not null, sub-classes must determine the final result
	 * of the compare by returning the value from a call to abstract method 
	 * extendedCompare. 
	 */
	public int compare(String a, String b) {
		int result = 0;
		String stringA = normalizeString((_sortAttributes.isAscendingOrder() ? a : b));
		String stringB = normalizeString((_sortAttributes.isAscendingOrder() ? b : a));

		if( stringA != null && stringB != null ) {
			result = extendedCompare(stringA, stringB);
		}
		else if( stringA == null && stringB == null ) {
			result = 0;
		}
		else if ( stringA == null ) {
			result = _sortAttributes.isNullLowSortOrder() ? -1 : 1;
		}
		else {    
			result = _sortAttributes.isNullLowSortOrder() ? 1 : -1;			
		}
		return result;
	}

	/**
	 * Normalize the String for sorting<p>
	 * 
	 * Normalizing involves transforming the original value so
	 * that zero length Strings are treated as nulls. It also
	 * involves stripping trailing and leading spaces from the 
	 * original, provided the isTrim attribute is set.
	 * 
	 * @param original the String to be normalized
	 * @return the normalized text
	 */
	private String normalizeString(String original) {
		String result = null;

		if( original != null ) {
			if( _sortAttributes.isTrim() ) {
				original = original.trim();
			}
			if( original.length() > 0 ) {
				result = original;
			}
		}		
		return result;
	}

	protected abstract int extendedCompare(String a, String b);
}
