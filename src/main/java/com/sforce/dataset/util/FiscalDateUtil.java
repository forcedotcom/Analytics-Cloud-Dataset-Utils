/*
 * Copyright (c) 2014, salesforce.com, inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided 
 * that the following conditions are met:
 * 
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the 
 *    following disclaimer.
 *  
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
 *    the following disclaimer in the documentation and/or other materials provided with the distribution. 
 *    
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior written permission.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.sforce.dataset.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author pgupta
 *
 */
public class FiscalDateUtil {


		static final SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd zzz yyyy");
		
	    /**
	     * @param date
	     * @param fiscalMonthOffset
	     * @return The fiscalMonth for the specified offset. 
	     * In java Month starts at 0, so add 1 to the result to get actual month
	     */
	    public static int getFiscalMonth(int month, int fiscalMonthOffset) {
//	        int month = date.get(Calendar.MONTH); 
	        int result = ((month - fiscalMonthOffset - 1) % 12) + 1;
	        if (result < 0) {
	            result += 12;
	        }
	        return result;
	    }

	    /**
	     * @param month
	     * @param year
	     * @param fiscalMonthOffset
	     * @param isYearEndFiscalYear
	     * @return
	     */
	    public static int getFiscalYear(int year,int month, int fiscalMonthOffset, boolean isYearEndFiscalYear) {
//	        int month = date.get(Calendar.MONTH);
//	        int year = date.get(Calendar.YEAR);
	        if(isYearEndFiscalYear)
	        	return (month >= fiscalMonthOffset) ? year +1: year;
	        else
	        	return (month >= fiscalMonthOffset) ? year : year - 1;
	    }

	    /**
	     * @param month
	     * @param fiscalMonthOffset
	     * @return
	     */
	    public static int getFiscalQuarter(int month,int fiscalMonthOffset) {
	    	return (int) ((((Math.ceil(1.0*(22-fiscalMonthOffset+month)/3.0)*3.0)/3)%4)+1);
	    }

	    /**
	     * @param date
	     * @param fiscalMonthOffset
	     * @param firstDayOfWeek
	     * @return
	     */
	    public static int getFiscalWeek(Calendar date, int fiscalMonthOffset, int firstDayOfWeek) {
	    	if(fiscalMonthOffset==0)
	    		return getCalendarWeek(date,firstDayOfWeek);
	    	int year = getCalendarYear(date);
	    	int month = getCalendarMonth(date);
		    long day_epoch = date.getTimeInMillis()/(1000*60*60*24);
	    	if(month<fiscalMonthOffset)
	    		year = year -1;
	    	Calendar fiscalNewYear = setDate(year,fiscalMonthOffset,1);
    		int minimalDaysInFirstFiscalWeek = getminimalDaysInFirstWeek(fiscalNewYear.get(Calendar.DAY_OF_WEEK), firstDayOfWeek);
		    long fiscalNewYear_day_epoch = fiscalNewYear.getTime().getTime()/(1000*60*60*24);
		    int dayofFiscalYear = (int) (1+(day_epoch-fiscalNewYear_day_epoch));
	        int fiscalWeek = getCalendarWeek(dayofFiscalYear, minimalDaysInFirstFiscalWeek);
	        return fiscalWeek;
	    }

	    /**
	     * @param date
	     * @return
	     */
	    public static int getCalendarYear(Calendar date) {
	        return date.get(Calendar.YEAR);
	    }

	    /**
	     * @param date
	     * @return the month number. In java month starts at 0
	     */
	    public static int getCalendarMonth(Calendar date) {
	        return date.get(Calendar.MONTH);
	    }
    
	    /**
	     * @param date
	     * @param firstDayOfWeek
	     * @return
	     */
	    public static int getCalendarWeek(Calendar date, int firstDayOfWeek) {
		    Calendar newYearsDay = FiscalDateUtil.setDate(getCalendarYear(date), 0, 1);
	    	int minimalDaysInFirstWeek = getminimalDaysInFirstWeek(newYearsDay.get(Calendar.DAY_OF_WEEK), firstDayOfWeek);
			int dayofYear = date.get(Calendar.DAY_OF_YEAR);
			if(dayofYear <=(minimalDaysInFirstWeek))
	    		return 1;
	    	int adjustedDayOfYear = dayofYear-(minimalDaysInFirstWeek)-1;
	    	int week = (adjustedDayOfYear/7 + 1)+1;
	    	return week;
	    }

	    /**
	     * @param dayofYear
	     * @param minimalDaysInFirstWeek
	     * @return
	     */
	    public static int getCalendarWeek(int dayofYear, int minimalDaysInFirstWeek) {
			if(dayofYear <=(minimalDaysInFirstWeek))
	    		return 1;
	    	int adjustedDayOfYear = dayofYear-(minimalDaysInFirstWeek)-1;
	    	int week = (adjustedDayOfYear/7 + 1)+1;
	    	return week;
	    }

	    /**
	     * @param month
	     * @return
	     */
	    public static int getCalendarQuarter(int month) {
	    	return getFiscalQuarter(month, 0);
	    }

	    /**
	     * @param year
	     * @param month
	     * @param day
	     * @return
	     */
	    public static Calendar setDate(int year, int month, int day) {
	        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
	        calendar.setLenient(false);
//	        Calendar calendar = Calendar.getInstance();
	        calendar.set(Calendar.YEAR, year);
	        calendar.set(Calendar.MONTH, month);
	        calendar.set(Calendar.DAY_OF_MONTH, day);
	        calendar.set(Calendar.HOUR_OF_DAY, 0);
	        calendar.set(Calendar.MINUTE, 0);
	        calendar.set(Calendar.SECOND, 0);
	        calendar.getTime();
	        return calendar;
	    }

	    
	/**
	 * @param newYearDayOfWeek
	 * @param firstDayOfWeek
	 * @return
	 */
	public static int getminimalDaysInFirstWeek(int newYearDayOfWeek, int firstDayOfWeek) 
	{
		if (firstDayOfWeek == -1)
			return 7;
		int minimalDaysInFirstWeek = (firstDayOfWeek - newYearDayOfWeek);
		if (minimalDaysInFirstWeek <= 0) {
			minimalDaysInFirstWeek += 7;
		}
		minimalDaysInFirstWeek++;
		if (minimalDaysInFirstWeek > 7) {
			minimalDaysInFirstWeek -= 7;
		}
		return minimalDaysInFirstWeek;
	}	    
	    
	    public static void main(String[] args) {
	    	int fiscalMonthOffset = 2;
//	    	int firstDayOfWeek = 0;
	    	int minimalDaysInFirstWeek = 7;
	    	boolean isYearEndFiscalYear = true;
	    	sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	    	
	    	for(int firstDayOfWeek = -1; firstDayOfWeek<7;firstDayOfWeek++)
	    	{
	    		int year = 2012;
		    	Calendar cal = setDate(year,0,1);
		    	cal.getTime();

	    		minimalDaysInFirstWeek = getminimalDaysInFirstWeek(cal.get(Calendar.DAY_OF_WEEK), firstDayOfWeek);

	    		System.out.println("\nfirstDayOfWeek: "+firstDayOfWeek+" minimalDaysInFirstWeek: "+minimalDaysInFirstWeek);
	    		
	    		System.out.println("Date\t\t\t\tFiscal Year\tFiscal Quarter\tCal Quarter\tFiscal Month\tFiscal Week\tCal Week");
		    	for(int i=0;i<365;i++)
		    	{
		    		System.out.print(sdf.format(cal.getTime())+"\t");
			        System.out.print(getFiscalYear(getCalendarYear(cal),getCalendarMonth(cal), fiscalMonthOffset, isYearEndFiscalYear)+"\t\t");
			        System.out.print(getFiscalQuarter(getCalendarMonth(cal),fiscalMonthOffset)+"\t\t"+getCalendarQuarter(getCalendarMonth(cal))+"\t\t");
			        System.out.print((getFiscalMonth(getCalendarMonth(cal), fiscalMonthOffset)+1)+"\t\t"); 
			        System.out.print(getFiscalWeek(cal, fiscalMonthOffset, firstDayOfWeek)+"\t\t");
			        System.out.println(getCalendarWeek(cal.get(Calendar.DAY_OF_YEAR), minimalDaysInFirstWeek));
		    		cal.add(Calendar.DAY_OF_YEAR, 1);
		    		cal.getTime();
		    	}

	    	}
	    	
}
	    
/*
	    public static int dow(int y, int m, int d)
	    {
	        int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
	        if(m < 2)
	        	y--;
	        return ((y + y/4 - y/100 + y/400 + t[m] + d) % 7)+1;
	    }
*/
}
