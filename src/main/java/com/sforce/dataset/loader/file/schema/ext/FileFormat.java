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
package com.sforce.dataset.loader.file.schema.ext;

public class FileFormat  extends com.sforce.dataset.loader.file.schema.FileFormat {
	
    public FileFormat()
    {
    	super();
    }
    
    public FileFormat(FileFormat old)
    {
    	super();
    	if(old!=null)
    	{
	    	this.charsetName = old.charsetName;
	    	this.fieldsDelimitedBy = old.fieldsDelimitedBy;
	    	this.fieldsEnclosedBy = old.fieldsEnclosedBy;
	    	this.linesTerminatedBy = old.linesTerminatedBy;
	    	this.numberOfLinesToIgnore = old.numberOfLinesToIgnore;
    	}
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((charsetName == null) ? 0 : charsetName.hashCode());
		result = prime
				* result
				+ ((fieldsDelimitedBy == null) ? 0 : fieldsDelimitedBy
						.hashCode());
		result = prime * result + fieldsEnclosedBy;
		result = prime
				* result
				+ ((linesTerminatedBy == null) ? 0 : linesTerminatedBy
						.hashCode());
		result = prime * result + numberOfLinesToIgnore;
		return result;
	}
	
    @Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FileFormat other = (FileFormat) obj;
		if (charsetName == null) {
			if (other.charsetName != null) {
				return false;
			}
		} else if (!charsetName.equals(other.charsetName)) {
			return false;
		}
		if (fieldsDelimitedBy == null) {
			if (other.fieldsDelimitedBy != null) {
				return false;
			}
		} else if (!fieldsDelimitedBy.equals(other.fieldsDelimitedBy)) {
			return false;
		}
		if (fieldsEnclosedBy != other.fieldsEnclosedBy) {
			return false;
		}
		if (linesTerminatedBy == null) {
			if (other.linesTerminatedBy != null) {
				return false;
			}
		} else if (!linesTerminatedBy.equals(other.linesTerminatedBy)) {
			return false;
		}
		if (numberOfLinesToIgnore != other.numberOfLinesToIgnore) {
			return false;
		}
		return true;
	}

}
