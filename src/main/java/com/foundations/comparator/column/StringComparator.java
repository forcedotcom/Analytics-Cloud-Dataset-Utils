package com.foundations.comparator.column;

import java.text.Normalizer;
import java.util.regex.Pattern;

import com.foundations.comparator.attributes.StringSortAttributes;

public final class StringComparator extends AbstractComparator {

	private boolean _isStripAccents;
	private boolean _isCaseSensitive;
	private Pattern _pattern;
	
	public StringComparator(String name, int sortOrder, StringSortAttributes attributes) {
		super(name, sortOrder, attributes);
		_isStripAccents = attributes.isStripAccents();
		_isCaseSensitive = attributes.isCaseSensitive();
		_pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
	}
	
	protected int extendedCompare(String a, String b) {
		
		if( _isStripAccents ) {
			a = stripAccents(a);
			b = stripAccents(b);
		}
		return _isCaseSensitive ? a.compareTo(b) : a.compareToIgnoreCase(b);
	}
	
	/**
     * <p>Removes diacritics (~= accents) from a string. The case will not be altered.</p>
     * <p>For instance, '&agrave;' will be replaced by 'a'.</p>
     * <p>Note that ligatures will be left as is.</p>
     *
     * <pre>
     * stripAccents(null) = null
     * stripAccents("") = ""
     * stripAccents("control") = "control"
     * stripAccents("&eacute;clair") = "eclair"
     * </pre>
     * This function is a modified version of stripAccents in 
     * org.apache.commons.lang3.StringUtils<p>
     *
     * @param input String to be stripped
     * @return input text with diacritics removed
     */	
    private String stripAccents(String input) {
        String decomposed = Normalizer.normalize(input, Normalizer.Form.NFD);
        return _pattern.matcher(decomposed).replaceAll("");
    }	
}
