package com.sforce.dataset.util;

import java.util.Comparator;

public class SortSimpleDateFormat implements Comparator<String > {

	@Override
	public int compare(String o1, String o2) {
		if (o1.length() > o2.length())
			return -1;
		else if (o1.length() < o2.length())
			return 1;
		else
		{
			return o1.compareTo(o2);
			
		}
	}

}


//public class SortSimpleDateFormat implements Comparator<SimpleDateFormat > {
//
//	@Override
//	public int compare(SimpleDateFormat o1, SimpleDateFormat o2) {
//		return o1.toPattern().compareTo(o2.toPattern());
//	}
//
//}
