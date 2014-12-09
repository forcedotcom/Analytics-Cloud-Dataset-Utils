package com.foundations.comparator.attributes;

public final class StringSortAttributes extends SortAttributes {
	
	public static final boolean DEFAULT_CASE_SENSITIVE = true;
	public static final boolean DEFAULT_STRIP_ACCENTS = false;

	private boolean _caseSensitive;
	private boolean _stripAccents;
	
	public StringSortAttributes() {
		_caseSensitive = DEFAULT_CASE_SENSITIVE;
		_stripAccents = DEFAULT_STRIP_ACCENTS;
	}

	public void setCaseSensitive(boolean value) {
		_caseSensitive = value;
	}

	public boolean isCaseSensitive() {
		return _caseSensitive;
	}

	public void setStripAccents(boolean value) {
		_stripAccents = value;		
	}

	public boolean isStripAccents() {
		return _stripAccents;
	}
}
