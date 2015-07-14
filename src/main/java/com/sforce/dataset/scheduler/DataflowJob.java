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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import com.sforce.dataset.DatasetUtilConstants;
import com.sforce.dataset.flow.DataFlow;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.flow.monitor.DataFlowMonitorUtil;
import com.sforce.dataset.flow.monitor.JobEntry;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

@DisallowConcurrentExecution
public class DataflowJob  implements Job {
	
	private String defaultDataflowId = null;
	
	public List<DataFlow> getDataFlowList(JobDataMap dataMap, PartnerConnection partnerConnection) throws ClientProtocolException, ConnectionException, URISyntaxException, IOException
	{
		List<DataFlow> list = new LinkedList<DataFlow>();
		List<DataFlow> dfList = DataFlowUtil.listDataFlow(partnerConnection);
		for(DataFlow df:dfList)
		{
			if(df.getName().equals("SalesEdgeEltWorkflow"))
			{
				defaultDataflowId = df.get_uid();
			}

			
			for(String key:dataMap.keySet())
			{
				if(key.equals(df.getName()))
				{
					if(df.getStatus().equalsIgnoreCase("Active"))
					{
						list.add(df);
					}else
					{
						System.out.println("Dataflow {"+df.getName()+"} is not active and cannot be scheduled");
					}
				}
			}
		}
		if(defaultDataflowId==null)
		{
			throw new IllegalArgumentException("Default dataflow not found");
		}
		if(list.isEmpty())
		{
			throw new IllegalArgumentException("No Active Dataflows found in schedule");
		}		
		return list;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException 
	{
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		PartnerConnection partnerConnection = null;
		try {
			partnerConnection = (PartnerConnection) context.getScheduler().getContext().get("conn");
		} catch (SchedulerException e1) {
			throw new JobExecutionException(e1); 
		}
		
		if(partnerConnection==null)
		{
			try {
				SchedulerUtil.disableSchedule(partnerConnection, context.getJobDetail().getKey().getName());
			} catch (Exception t) {
				t.printStackTrace();
			}
			throw new JobExecutionException("No Connection info found");
		}

		List<DataFlow> tasks = null;
		try {
			tasks = getDataFlowList(dataMap, partnerConnection);
		} catch (Exception e) {
			try {
				SchedulerUtil.disableSchedule(partnerConnection, context.getJobDetail().getKey().getName());
			} catch (Exception t) {
				t.printStackTrace();
			}
			throw new JobExecutionException(e);
		}
		
		for(DataFlow task:tasks)
		{
			Session session = null;
			try {
				session = Session.getCurrentSession(partnerConnection.getUserInfo().getOrganizationId(), task.getMasterLabel(), true);
				session.setType("Dataflow");
				session.start();
				runDataflow(task,partnerConnection);
				session.end();
				session.setParam(DatasetUtilConstants.serverStatusParam,"COMPLETED");
			} catch (Exception e) {
//				e.printStackTrace();
				session.fail(e.getMessage());
				throw new JobExecutionException(e); //TODO Do we cary on to next job
			}
		}
	}
	
	
	@SuppressWarnings("rawtypes")
	public void runDataflow(DataFlow task, PartnerConnection partnerConnection) throws IllegalStateException, ConnectionException, IOException, URISyntaxException
	{		
		System.out.println(new Date()+ " : Executing job: " + task.getName());
		if(!isRunning(defaultDataflowId, 0, partnerConnection, task.getName()))
		{
			long startTime = System.currentTimeMillis();
//			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
//			long utcStartTime = cal.getTimeInMillis();
//			System.out.println("Difference between current and utc:" + (utcStartTime-startTime));
			if(task.getWorkflowType().equalsIgnoreCase("local"))
			{
				DataFlowUtil.uploadDataFlow(partnerConnection, task.getName(), defaultDataflowId, task.getWorkflowDefinition());
			}
			DataFlowUtil.startDataFlow(partnerConnection, task.getName(), defaultDataflowId);			
			while(true)
			{
				if(isRunning(defaultDataflowId, startTime, partnerConnection, task.getName()))
				{
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}else
				{
					break;
				}
			}
			if(task.getWorkflowType().equalsIgnoreCase("local"))
			{
				DataFlowUtil.uploadDataFlow(partnerConnection, "Empty Dataflow", defaultDataflowId, new HashMap());
			}
		}else
		{
			throw new IllegalStateException("Dataflow is already running");
		}
		
		
	}
	
	public boolean isRunning(String Id, long after, PartnerConnection partnerConnection, String dataFlowName) throws ClientProtocolException, ConnectionException, URISyntaxException, IOException
	{		
		boolean jobFound = false;
		while(!jobFound)
		{
			List<JobEntry> jobList = DataFlowMonitorUtil.getDataFlowJobs(partnerConnection, null, Id);
			for(JobEntry job:jobList)
			{
				if(job.getStartTimeEpoch()>after)
				{
					jobFound = true;
					if(job.getStatus()==2)
					{
						return true;
					}else
					{
						if(after>0)
						{
							if(job.getStatus()==1)
							{
								System.out.println(new Date()+ " Scheduled job {"+dataFlowName+"} completed succesfully");
							}else
							{
								System.out.println(new Date()+ " Scheduled job {"+dataFlowName+"} Failed");
							}
						}
					}
				}
			}
			if(!jobFound)
			{
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}

}
