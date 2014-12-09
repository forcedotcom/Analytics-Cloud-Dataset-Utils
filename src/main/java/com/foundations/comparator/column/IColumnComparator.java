package com.foundations.comparator.column;

import java.util.Comparator;

public interface IColumnComparator extends Comparator<String> {
	
	public int getSortOrder();
}
