package com.foundations.comparator.column;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.foundations.comparator.attributes.DateTimeSortAttributes;

public final class DateTimeComparator extends AbstractComparator {

	private SimpleDateFormat _formatter;
	
	public DateTimeComparator(String name, int sortOrder, DateTimeSortAttributes sortAttributes) {
		super(name, sortOrder, sortAttributes);
		_formatter = new SimpleDateFormat(sortAttributes.getPattern());
	}

	protected int extendedCompare(String a, String b) {
		int result;
		
		try {
			Date aValue = _formatter.parse(a);
			Date bValue = _formatter.parse(b);	
			result = aValue.compareTo(bValue);
		} 
		catch (ParseException e) {
			throw new RuntimeException("Parse Exception: " + e.getMessage());
		} 

		return result;
	}
}

 
//////////////////////////  USE FOLLOWING CODE FOR JAVA 8 ///////
// import java.time.LocalDateTime;
// import java.time.format.DateTimeFormatter;
// 
// public final class DateTimeComparator extends AbstractComparator {
// 
// 	private DateTimeFormatter _formatter;
// 	
// 	public DateTimeComparator(String name, int sortOrder, DateTimeSortAttributes sortAttributes) {
// 		super(name, sortOrder, sortAttributes);
// 		_formatter = DateTimeFormatter.ofPattern(sortAttributes.getPattern());
// 	}
// 
// 	protected int extendedCompare(String a, String b) {
// 		LocalDateTime aValue = LocalDateTime.parse(a, _formatter);  
// 		LocalDateTime bValue = LocalDateTime.parse(b, _formatter);  
// 		
// 		return aValue.compareTo(bValue);
// 	}
// }
//////////////////////////USE FOLLOWING CODE FOR JAVA 8 ///////
