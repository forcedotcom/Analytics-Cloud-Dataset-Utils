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
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.sforce.dataset.flow.DataFlow;
import com.sforce.dataset.flow.DataFlowUtil;
import com.sforce.dataset.flow.monitor.DataFlowMonitorUtil;
import com.sforce.dataset.flow.monitor.JobEntry;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

@DisallowConcurrentExecution
public class DataflowJob  implements Job {
	
	private final PartnerConnection partnerConnection;
	private final List<DataFlow> tasks;
	private final String dataflowId;
	
	public DataflowJob(List<DataFlow> tasks, PartnerConnection partnerConnection) throws ClientProtocolException, ConnectionException, URISyntaxException, IOException
	{
		if(tasks == null || tasks.isEmpty())
		{
			throw new IllegalArgumentException("Parameter tasks cant be null or empty");
		}
		if(partnerConnection == null)
		{
			throw new IllegalArgumentException("partnerConnection cant be null");
		}
		this.tasks = tasks;
		this.partnerConnection = partnerConnection;
		boolean found = false;
		List<DataFlow> dfList = DataFlowUtil.listDataFlow(partnerConnection);
		String tempId = null;
		for(DataFlow df:dfList)
		{
			if(df.getName().equals("SalesEdgeEltWorkflow"))
			{
				tempId = df.get_uid();
				found = true;
				break;
			}
		}
		if(!found)
		{
			throw new IllegalArgumentException("Default dataflow not found");
		}
		dataflowId = tempId;
	}

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		
		for(DataFlow task:tasks)
		{
			if(tasks.size()==1)
			{
				if(task.getWorkflowType().equalsIgnoreCase("user"))
				{
					try {
						runDataflow(task);
					} catch (Exception e) {
						throw new JobExecutionException(e);
					}
				}
			}
		}
	}
	
	
	public void runDataflow(DataFlow task) throws IllegalStateException, ConnectionException, IOException, URISyntaxException
	{		
		if(!isRunning(dataflowId, 0))
		{
			long startTime = System.currentTimeMillis();
			DataFlowUtil.startDataFlow(partnerConnection, task.getName(), task.get_uid());			
			while(true)
			{
				if(isRunning(dataflowId, startTime))
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
		}else
		{
			throw new IllegalStateException("Dataflow is already running");
		}
		
		
	}
	
	public boolean isRunning(String Id, long after) throws ClientProtocolException, ConnectionException, URISyntaxException, IOException
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
