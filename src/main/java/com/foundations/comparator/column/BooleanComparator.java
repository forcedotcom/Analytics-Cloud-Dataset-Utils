package com.foundations.comparator.column;

import com.foundations.comparator.attributes.SortAttributes;

public class BooleanComparator extends AbstractComparator {

	public BooleanComparator(String name, int sortOrder, SortAttributes attributes) {
		super(name, sortOrder, attributes);
	}

	protected int extendedCompare(String a, String b) {
	    Boolean aValue = new Boolean(parse(a));
		Boolean bValue = new Boolean(parse(b));

		return aValue.compareTo(bValue);
	}
	
	private boolean parse(String value) {
		boolean result = false;
		
		if ( value.toLowerCase().equals("true") || value.equals("1") ) {
			result = true;
		}
		else if ( value.toLowerCase().equals("false") || value.equals("0") ) {
			result = false;
		}
		else {
			throw new RuntimeException( "Boolean Parse Exception: " + value);
		}
		return result;
	}
}
