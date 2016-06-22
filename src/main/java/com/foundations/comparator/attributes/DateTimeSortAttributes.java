package com.foundations.comparator.attributes;

public final class DateTimeSortAttributes extends SortAttributes {
	
	public static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	private String _pattern;

	public DateTimeSortAttributes() {
		_pattern = DEFAULT_PATTERN;
	}
	
	/**
	 * The date and time pattern used for the column to be sorted. 
	 *  
	 * Pattern syntax is based on java.text.SimpleDateFormat
	 * class documentation
	 */
	public void setPattern(String value) {
		_pattern = value;
	}

	/**
	 * Returns the date and time pattern for the column to be sorted.
	 */
	public String getPattern() {
		return _pattern;
	}
}
