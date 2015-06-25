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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
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
					list.add(sched);
				} catch (Exception e) {
					e.printStackTrace();
				}
			 }
		}
		return list;
	}
	
	public static void saveSchedule(PartnerConnection partnerConnection, String scheduleName, String jobType, String jobId, String cronSchedule, Map<?,?> jobDataMap) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getScheduleDir(orgId);
		String masterLabel = scheduleName;
		String devName = ExternalFileSchema.createDevName(scheduleName, "Schedule", 1, false);
		File file = new File(dir,devName+".json");
		Schedule dg = new Schedule();
		dg.setDevName(devName);
		dg.setMasterLabel(masterLabel);
		dg.setCronSchedule(cronSchedule);
		dg.setJobType(jobType);
		dg.setJobId(jobId);
		dg.setJobDataMap(jobDataMap);
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.writerWithDefaultPrettyPrinter().writeValue(file, dg);
		return;
	}
	

	public static void deleteSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getDataflowGroupDir(orgId);
		String devName = ExternalFileSchema.createDevName(scheduleName, "dataFlowGroup", 1, false);
		File file = new File(dir,devName+".json");
		if(file.exists())
		{
			file.delete();
		}else
		{
			throw new IllegalArgumentException("Schedule {"+scheduleName+"} does not exist in the system");
		}
	}
	
	
	public static void disableSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{


	}
	
	public static void enableSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException
	{


	}
	

	public static void startSchedule(PartnerConnection partnerConnection, String scheduleName) throws ConnectionException, JsonGenerationException, JsonMappingException, IOException, SchedulerException
	{
		String orgId = partnerConnection.getUserInfo().getOrganizationId();
		File dir = DatasetUtilConstants.getDataflowGroupDir(orgId);
		String devName = ExternalFileSchema.createDevName(scheduleName, "dataFlowGroup", 1, false);
		File file = new File(dir,devName+".json");
		if(!file.exists())
		{
			throw new IllegalArgumentException("Schedule {"+scheduleName+"} does not exist in the system");
		}
		ObjectMapper mapper = new ObjectMapper();	
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Schedule sched = mapper.readValue(file, Schedule.class);
		
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
               
        if(sched.getJobType().equalsIgnoreCase("dataflowGroup"))
        {
        	if(!scheduler.checkExists(jobKey(sched.getDevName(), "dataflowGroup")))
        	{
        		JobDetail job  = newJob(DataflowJob.class)
    			    .withIdentity(sched.getDevName(), "dataflowGroup")
    			    .build();

        		if(sched.getJobDataMap()!=null)
        		{
	        		for(Object key:sched.getJobDataMap().keySet())
	        		{
	        			job.getJobDataMap().put(key.toString(), sched.getJobDataMap().get(key));   
	        		}
        		}
        		
                CronTrigger trigger = newTrigger()
                	    .withIdentity(sched.getDevName()+"_trigger", "dataflowGroup")
                	    .withSchedule(cronSchedule(sched.getCronSchedule()))
                	    .build();
                
                scheduler.scheduleJob(job, trigger);
        	}
        }else
        {
        	throw new IllegalArgumentException("schedule {"+scheduleName+"} is already started");
        }

        if(scheduler.isStarted())
        	scheduler.start();		
	}



}
