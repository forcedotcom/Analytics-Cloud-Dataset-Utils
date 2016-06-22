package com.foundations.comparator.attributes;

public final class DateTimeSortAttributes extends SortAttributes {
	
	public static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	private String _pattern;

	public DateTimeSortAttributes() {
		_pattern = DEFAULT_PATTERN;
	}
	
	public void setPattern(String value) {
		_pattern = value;
	}

	public String getPattern() {
		return _pattern;
	}
}
