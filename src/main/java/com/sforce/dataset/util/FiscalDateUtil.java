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
import java.util.LinkedHashMap;
import java.util.TimeZone;

/**
 * The Class FiscalDateUtil.
 */
public class FiscalDateUtil {


		/** The Constant sdf. */
		static final SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd zzz yyyy HH:mm:ss.SSS");
		static
		{
	    	sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		}
		
	    /**
    	 * Gets the fiscal month.
    	 *
    	 * @param month the month
    	 * @param fiscalMonthOffset the fiscal month offset
    	 * @return the fiscal month
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
    	 * Gets the fiscal year.
    	 *
    	 * @param year the year
    	 * @param month the month
    	 * @param fiscalMonthOffset the fiscal month offset
    	 * @param isYearEndFiscalYear the is year end fiscal year
    	 * @return the fiscal year
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
    	 * Gets the fiscal quarter.
    	 *
    	 * @param month the month
    	 * @param fiscalMonthOffset the fiscal month offset
    	 * @return the fiscal quarter
    	 */
	    public static int getFiscalQuarter(int month,int fiscalMonthOffset) {
	    	return (int) ((((Math.ceil(1.0*(22-fiscalMonthOffset+month)/3.0)*3.0)/3)%4)+1);
	    }

	    /**
    	 * Gets the fiscal week.
    	 *
    	 * @param date the date
    	 * @param fiscalMonthOffset the fiscal month offset
    	 * @param firstDayOfWeek the first day of week
    	 * @return the fiscal week
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
    	 * Gets the calendar year.
    	 *
    	 * @param date the date
    	 * @return the calendar year
    	 */
	    public static int getCalendarYear(Calendar date) {
	        return date.get(Calendar.YEAR);
	    }

	    /**
    	 * Gets the calendar month.
    	 *
    	 * @param date the date
    	 * @return the calendar month
    	 */
	    public static int getCalendarMonth(Calendar date) {
	        return date.get(Calendar.MONTH);
	    }
    
	    /**
    	 * Gets the calendar week.
    	 *
    	 * @param date the date
    	 * @param firstDayOfWeek the first day of week
    	 * @return the calendar week
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
    	 * Gets the calendar week.
    	 *
    	 * @param dayofYear the dayof year
    	 * @param minimalDaysInFirstWeek the minimal days in first week
    	 * @return the calendar week
    	 */
	    public static int getCalendarWeek(int dayofYear, int minimalDaysInFirstWeek) {
			if(dayofYear <=(minimalDaysInFirstWeek))
	    		return 1;
	    	int adjustedDayOfYear = dayofYear-(minimalDaysInFirstWeek)-1;
	    	int week = (adjustedDayOfYear/7 + 1)+1;
	    	return week;
	    }

	    /**
    	 * Gets the calendar quarter.
    	 *
    	 * @param month the month
    	 * @return the calendar quarter
    	 */
	    public static int getCalendarQuarter(int month) {
	    	return getFiscalQuarter(month, 0);
	    }

	    /**
    	 * Sets the date.
    	 *
    	 * @param year the year
    	 * @param month the month
    	 * @param day the day
    	 * @return the calendar
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
	        calendar.set(Calendar.MILLISECOND, 0);
	        calendar.getTime();
	        return calendar;
	    }

	    
	/**
	 * Gets the minimal days in first week.
	 *
	 * @param newYearDayOfWeek the new year day of week
	 * @param firstDayOfWeek the first day of week
	 * @return the minimal days in first week
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
	    
	    /**
    	 * The main method.
    	 *
    	 * @param args the arguments
    	 */
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

	    		System.out.println("\nfirstDayOfWeek: "+firstDayOfWeek+"\t\tminimalDaysInFirstWeek: "+minimalDaysInFirstWeek+"\t\tfiscalMonthOffset: "+fiscalMonthOffset);
	    		System.out.println("");
	    		System.out.println("Date\t\t                Cal Year\tFiscal Year\tCal Quarter\tFiscal Quarter\tCal Month\tFiscal Month\tCal Week\tFiscal Week");
		    	for(int i=0;i<365;i++)
		    	{
		    		System.out.print(sdf.format(cal.getTime())+"\t\t");
			        System.out.print(getCalendarYear(cal)+"\t\t");
			        System.out.print(getFiscalYear(getCalendarYear(cal),getCalendarMonth(cal), fiscalMonthOffset, isYearEndFiscalYear)+"\t\t");
			        System.out.print(getCalendarQuarter(getCalendarMonth(cal))+"\t\t");
			        System.out.print(getFiscalQuarter(getCalendarMonth(cal),fiscalMonthOffset)+"\t\t");
			        System.out.print((getCalendarMonth(cal)+1)+"\t\t"); 
			        System.out.print((getFiscalMonth(getCalendarMonth(cal), fiscalMonthOffset)+1)+"\t\t"); 
			        System.out.print(getCalendarWeek(cal.get(Calendar.DAY_OF_YEAR), minimalDaysInFirstWeek)+"\t\t");
			        System.out.println(getFiscalWeek(cal, fiscalMonthOffset, firstDayOfWeek));
		    		cal.add(Calendar.DAY_OF_YEAR, 1);
		    		cal.getTime();
		    	}

	    	}
	    	
}
	
	    
	    /**
    	 * Gets the fiscal period start and end.
    	 *
    	 * @param row the row
    	 * @param cal the cal
    	 * @param fiscalMonthOffset the fiscal month offset
    	 * @param firstDayOfWeek the first day of week
    	 */
    	public static void getFiscalPeriodStartAndEnd(LinkedHashMap<String,Object> row,Calendar cal, int fiscalMonthOffset, int firstDayOfWeek)
	    {
//		    int day = cal.get(Calendar.DAY_OF_MONTH);
	    	@SuppressWarnings("unused")
			int dayOfweek = cal.get(Calendar.DAY_OF_WEEK);
		    int month = cal.get(Calendar.MONTH);
		    int year = cal.get(Calendar.YEAR);
		    int fiscal_year = year;
		    
	    	if(month<fiscalMonthOffset)
	    		fiscal_year = year -1;
	    	
	    	Calendar yearStart = setDate(fiscal_year,fiscalMonthOffset,1);
	    	long fiscal_year_start_epoch = yearStart.getTime().getTime();
	    	yearStart.add(Calendar.YEAR, 1);
	    	long fiscal_year_end_epoch = yearStart.getTime().getTime()-1;
	    	
	    	Calendar monthStart = setDate(year,month,1);
	    	long fiscal_month_start_epoch = monthStart.getTime().getTime();
	    	monthStart.add(Calendar.MONTH, 1);
	    	long fiscal_month_end_epoch = monthStart.getTime().getTime()-1;
	    	
	    	int fiscal_quarter = getFiscalQuarter(month, fiscalMonthOffset);
	    	
	    	Calendar quarterStart = setDate(year,fiscalMonthOffset+(fiscal_quarter-1)*3,1);
	    	long fiscal_quarter_start_epoch = quarterStart.getTime().getTime();
	    	quarterStart.add(Calendar.MONTH, 3);
	    	long fiscal_quarter_end_epoch = quarterStart.getTime().getTime()-1;
	    	
	    	Calendar fiscalNewYear = setDate(fiscal_year,fiscalMonthOffset,1);
    		int minimalDaysInFirstFiscalWeek = getminimalDaysInFirstWeek(fiscalNewYear.get(Calendar.DAY_OF_WEEK), firstDayOfWeek);

	    	int fiscalweek = getFiscalWeek(cal, fiscalMonthOffset, firstDayOfWeek);
	    	long fiscal_week_start_epoch = 0L;
	    	long fiscal_week_end_epoch = 0L;
	    	if(fiscalweek==1)
	    	{
		    	fiscal_week_start_epoch = fiscalNewYear.getTime().getTime();	    		
	    		fiscalNewYear.add(Calendar.DAY_OF_YEAR, minimalDaysInFirstFiscalWeek);
	    		fiscal_week_end_epoch = fiscalNewYear.getTime().getTime()-1;
	    	}else
	    	{
	    		fiscalNewYear.add(Calendar.DAY_OF_YEAR, minimalDaysInFirstFiscalWeek + 7*(fiscalweek-2));
		    	fiscal_week_start_epoch = fiscalNewYear.getTime().getTime();	    		
	    		fiscalNewYear.add(Calendar.DAY_OF_YEAR, 7);
	    		fiscal_week_end_epoch = fiscalNewYear.getTime().getTime()-1;

	    		if(fiscal_week_start_epoch<fiscal_year_end_epoch && fiscal_week_end_epoch > fiscal_year_end_epoch)
		    	{
		    		fiscal_week_end_epoch =  fiscal_year_end_epoch;
		    	}

	    	}

	    	System.out.println(sdf.format(cal.getTime()));
	    	System.out.println("Year start:"+sdf.format(fiscal_year_start_epoch)+", Year end:"+sdf.format(fiscal_year_end_epoch));
	    	System.out.println("Quarter start:"+sdf.format(fiscal_quarter_start_epoch)+", Quarter end:"+sdf.format(fiscal_quarter_end_epoch));
	    	System.out.println("Month start:"+sdf.format(fiscal_month_start_epoch)+", Month end:"+sdf.format(fiscal_month_end_epoch));
	    	System.out.println("Week start:"+sdf.format(fiscal_week_start_epoch)+", Week end:"+sdf.format(fiscal_week_end_epoch));
	    	
	    }
}
