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
package com.sforce.dataset.scheduler;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;


public class SchedulerUtil {
	
	public static List<Schedule> listSchedules(PartnerConnection partnerConnection) throws ConnectionException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		List<Schedule> list = new LinkedList<Schedule>();
		File dir = DatasetUtilConstants.getScheduleDir(orgId);
		IOFileFilter suffixFileFilter = FileFilterUtils.suffixFileFilter(".json", IOCase.INSENSITIVE);
		File[] fileList =	DatasetUtils.getFiles(dir, suffixFileFilter);
		if(fileList!=null)
		{
			ObjectMapper mapper = new ObjectMapper();	
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 for(File file:fileList)
			 {
				 try {
					 Schedule sched = mapper.readValue(file, Schedule.class);
					 if(sched.getDevName()!=null)
					 {
						 if(!sched.isDisabled())
						 {
							 sched.setNextRunTime(getScheduledNextRunTime(partnerConnection, sched));
						 }
						 list.add(sched);
					 }
				} catch (Exception e) {
					e.printStackTrace();
				}
			 }
		}
		return list;
	}
	
	public static void saveSchedule(PartnerConnection partnerConnection, String scheduleName, String cronSchedule, String frequency,long interval, Map<?,?> jobDataMap, long scheduleStartDate, boolean isCreate) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String masterLabel = scheduleName;
		String devName = ExternalFileSchema.createDevName(scheduleName, "Schedule", 1, false);
		Schedule dg = new Schedule();
		dg.setDevName(devName);
		dg.setMasterLabel(masterLabel);
		dg.setFrequency(frequency);
		dg.setInterval(interval);
		dg.setScheduleStartDate(scheduleStartDate);
		dg.setCronSchedule(cronSchedule);
		dg.setJobDataMap(jobDataMap);
		dg.set_LastModifiedBy(partnerConnection);
		writeSchedule(partnerConnection, dg, isCreate);
		return;
	}
	

	public static void deleteSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getScheduleDir(orgId);
		String devName = ExternalFileSchema.createDevName(scheduleName, "schedule", 1, false);
		File file = new File(dir,devName+".json");
		if(file.exists())
		{
			disableSchedule(partnerConnection, devName);
			file.delete();
		}else
		{
			throw new IllegalArgumentException("Schedule {"+scheduleName+"} does not exist in the system");
		}
	}
	
	
	public static void disableSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		Schedule sched = readSchedule(partnerConnection, scheduleName);
		stopSchedule(partnerConnection, sched);
    	sched.setDisabled(true);
    	writeSchedule(partnerConnection, sched, false);
	}
	
	public static void enableSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		Schedule sched = readSchedule(partnerConnection, scheduleName);  
		startSchedule(sched, partnerConnection);
        sched.setDisabled(false);
        writeSchedule(partnerConnection, sched, false);
	}
	
	public static void writeSchedule(PartnerConnection partnerConnection, Schedule schedule, boolean isCreate) throws ConnectionException, JsonParseException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getScheduleDir(orgId);
		File file = new File(dir,schedule.getDevName()+".json");
		if(isCreate && file.exists())
		{
			throw new IllegalArgumentException("Schedule {"+schedule.getDevName()+"} already exists in the system");
		}
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(file, schedule);
	}

	public static Schedule readSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonParseException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getScheduleDir(orgId);
		String devName = ExternalFileSchema.createDevName(scheduleName, "schedule", 1, false);
		File file = new File(dir,devName+".json");
		if(!file.exists())
		{
			throw new IllegalArgumentException("Schedule {"+scheduleName+"} does not exist in the system");
		}
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Schedule sched = mapper.readValue(file, Schedule.class);
		return sched;
	}

//	public static void startSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
//	{
////		String orgId = partnerConnection.getUserInfo().getOrganizationId();
////		File dir = DatasetUtilConstants.getScheduleDir(orgId);
////		String devName = ExternalFileSchema.createDevName(scheduleName, "schedule", 1, false);
////		File file = new File(dir,devName+".json");
////		if(!file.exists())
////		{
////			throw new IllegalArgumentException("Schedule {"+scheduleName+"} does not exist in the system");
////		}
////		ObjectMapper mapper = new ObjectMapper();	
////		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//
//		Schedule sched = readSchedule(partnerConnection, scheduleName);
//		
//        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
//               
//        if(sched.getJobType().equalsIgnoreCase("schedule"))
//        {
//        	if(!scheduler.checkExists(jobKey(sched.getDevName(), "schedule")))
//        	{
//        		JobDetail job  = newJob(DataflowJob.class)
//    			    .withIdentity(sched.getDevName(), "schedule")
//    			    .build();
//
//        		if(sched.getJobDataMap()!=null)
//        		{
//	        		for(Object key:sched.getJobDataMap().keySet())
//	        		{
//	        			job.getJobDataMap().put(key.toString(), sched.getJobDataMap().get(key));   
//	        		}
//        		}
//        		
//                CronTrigger trigger = newTrigger()
//                	    .withIdentity(sched.getDevName()+"_trigger", "schedule")
//                	    .withSchedule(cronSchedule(sched.getCronSchedule()))
//                	    .build();
//                
//                scheduler.scheduleJob(job, trigger);
//        	}
//        }else
//        {
//        	throw new IllegalArgumentException("schedule {"+scheduleName+"} is already started");
//        }
//
//        if(scheduler.isStarted())
//        	scheduler.start();		
//	}

	
	public static String getCronSchedule(Schedule sched)
	{
		if(sched!=null)
		{
			if(sched.getCronSchedule()!=null && !sched.getCronSchedule().isEmpty())
				return sched.getCronSchedule();
			else
			{
				Date dt = new Date(sched.getScheduleStartDate());
				Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
				calendar.setTime(dt);   // assigns calendar to given date 
				int minute = calendar.get(Calendar.MINUTE);
				int hour = calendar.get(Calendar.HOUR_OF_DAY);
				int day = calendar.get(Calendar.DAY_OF_MONTH);
				int dayofweek = calendar.get(Calendar.DAY_OF_WEEK);
				long interval = sched.getInterval();
				
				if(sched.getFrequency().equalsIgnoreCase("day"))
				{
					if(interval == 1)
						return String.format("0 %d %d * * ?",minute,hour);
					else
						return String.format("0 %d %d * * ?",minute,hour);
						
				} else if(sched.getFrequency().equalsIgnoreCase("hour"))
				{
					if(interval == 1)
						return String.format("0 %d * * * ?",minute);
					else
						return String.format("0 %d */%d * * ?",minute,interval);
				} else if(sched.getFrequency().equalsIgnoreCase("minute"))
				{
					if(interval == 1)
						return "0 * * * * ?";
					else
						return String.format("0 */%d * * * ?",interval);
				} else if(sched.getFrequency().equalsIgnoreCase("week"))
				{
					if(interval == 1)
						return String.format("0 %d %d ? * %d",minute,hour,dayofweek);
					else
						return String.format("0 %d %d ? * %d",minute,hour,dayofweek);
				} else if(sched.getFrequency().equalsIgnoreCase("month"))
				{
					if(interval == 1)
						return String.format("0 %d %d %d * ?",minute,hour,day);
					else
						return String.format("0 %d %d %d * ?",minute,hour,day);
				}
			}
		}		
		return null;
	}
	
	public static void startAllSchedules(PartnerConnection partnerConnection) throws ConnectionException, SchedulerException
	{
		List<Schedule> list = listSchedules(partnerConnection);
		for(Schedule sched:list)
		{
			if(!sched.isDisabled())
			{
				try
				{
					startSchedule(sched, partnerConnection);
				}catch(Exception t)
				{
					t.printStackTrace();
				}
			}
		}
	}
	
	public static void startSchedule(Schedule sched, PartnerConnection partnerConnection) throws SchedulerException
	{
	       Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
	        if(sched.getJobType().equalsIgnoreCase("dataflow"))
	        {
	        	JobKey jkey = jobKey(sched.getDevName(), "dataflow");

	        	if(!scheduler.checkExists(jkey))
	        	{
	        		JobDetail job  = newJob(DataflowJob.class)
	    			    .withIdentity(sched.getDevName(), "dataflow")
	    			    .build();

	        		if(sched.getJobDataMap()!=null)
	        		{
		        		for(Object key:sched.getJobDataMap().keySet())
		        		{
		        			job.getJobDataMap().put(key.toString(), sched.getJobDataMap().get(key));   
		        		}
	        		}
	        		        		
	                CronTrigger trigger = newTrigger()
	                	    .withIdentity(sched.getDevName()+"_trigger", "dataflow")
	                	    .withSchedule(cronSchedule(getCronSchedule(sched)))
	                	    .build();
	                
	                scheduler.getContext().put("conn", partnerConnection);
	                
	                Date temp = scheduler.scheduleJob(job, trigger);
	                
	                System.out.println("Schedule job {"+sched.getDevName()+"} scheduled next run {"+temp+"}");
	                
	                if(!scheduler.isStarted())
	                	scheduler.start();	
	        	}
	        }
	}
	
	
	public static void stopSchedule(PartnerConnection partnerConnection, Schedule sched) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        if(sched.getJobType().equalsIgnoreCase("dataflow"))
        {
        	JobKey jkey = jobKey(sched.getDevName(), "dataflow");
        	if(scheduler.checkExists(jkey))
        	{        
        		scheduler.deleteJob(jkey);
        	}
        }
	}
	
	public static long getScheduledNextRunTime(PartnerConnection partnerConnection, Schedule sched) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        if(sched.getJobType().equalsIgnoreCase("dataflow"))
        {
        	JobKey jkey = jobKey(sched.getDevName(), "dataflow");
        	if(scheduler.checkExists(jkey))
        	{        
                List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jkey);
                if (triggers.size() > 0)
                {
                    Date dt = triggers.get(0).getNextFireTime();
                    return dt.getTime();
                }
            }
        }
		return 0;
	}
	



}
