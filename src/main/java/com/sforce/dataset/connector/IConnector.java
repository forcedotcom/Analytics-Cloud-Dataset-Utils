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
package com.sforce.dataset.connector;

import java.util.List;
import java.util.UUID;

import com.sforce.dataset.connector.exception.ConnectionException;
import com.sforce.dataset.connector.exception.DataConversionException;
import com.sforce.dataset.connector.exception.DataReadException;
import com.sforce.dataset.connector.exception.DataWriteException;
import com.sforce.dataset.connector.exception.FatalException;
import com.sforce.dataset.connector.exception.MetadataException;
import com.sforce.dataset.connector.metadata.FieldType;
import com.sforce.dataset.connector.metadata.ObjectType;

public interface IConnector {
	
	public UUID connectorID = null;
	public String connectorName = null;
	public String connectorType = null;
		
	public UUID getConnectorID();
	
	public String getConnectorName();

	public String getConnectorType();

	public List<ObjectType> getObjectList(boolean isSource) throws MetadataException;
	
	public List<FieldType> getFieldList(ObjectType object, boolean isSource) throws MetadataException;
	
	public void read(OutputPipeline buffer, ObjectType object, List<FieldType> fields, int batchSize) throws ConnectionException, DataReadException, DataConversionException, FatalException;
	
	public void write(InputPipeline buffer, WriteOperation operation, ObjectType object, List<FieldType> fields) throws ConnectionException, DataWriteException, DataConversionException, FatalException;
	
	public void connect() throws ConnectionException;
	
	public void disconnect() throws ConnectionException;
}
