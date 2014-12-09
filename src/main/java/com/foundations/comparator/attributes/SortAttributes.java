package com.foundations.comparator.attributes;

public class SortAttributes {
	
	public static final boolean DEFAULT_ASCENDING_ORDER = true;
	public static final boolean DEFAULT_NULL_LOW_SORT_ORDER = true;
	public static final boolean DEFAULT_TRIM = false;
	
	private boolean _ascendingOrder;
	private boolean _trim;	
	private boolean _nullLowSortOrder;
	
	public SortAttributes() {
		_ascendingOrder = DEFAULT_ASCENDING_ORDER;
		_trim = DEFAULT_TRIM;
		_nullLowSortOrder = DEFAULT_NULL_LOW_SORT_ORDER;
	}
	
	public void setAscendingOrder(boolean value) {
		_ascendingOrder = value;
	}

	public boolean isAscendingOrder() {
		return _ascendingOrder;
	}
	
	public void setTrim(boolean value) {
		_trim = value;
	}

	public boolean isTrim() {
		return _trim;
	}

	public void setNullLowSortOrder(boolean value) {
		_nullLowSortOrder = value;
	}

	public boolean isNullLowSortOrder() {
		return _nullLowSortOrder;
	}
}
