package com.foundations.comparator.column;

import java.math.BigInteger;

import com.foundations.comparator.attributes.SortAttributes;

public final class IntegerComparator extends AbstractComparator {

	public IntegerComparator(String name, int sortOrder, SortAttributes attributes) {
		super(name, sortOrder, attributes);
	}

	protected int extendedCompare(String a, String b) {
	    BigInteger aValue = new BigInteger(a);
		BigInteger bValue = new BigInteger(b);

		return aValue.compareTo(bValue);
	}
}
