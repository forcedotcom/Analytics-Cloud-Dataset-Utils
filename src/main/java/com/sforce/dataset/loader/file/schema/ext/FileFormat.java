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
	    	setCharsetName( old.getCharsetName());
	    	setFieldsDelimitedBy(old.getFieldsDelimitedBy());
	    	setFieldsEnclosedBy(old.getFieldsEnclosedBy());
	    	setLinesTerminatedBy(old.getLinesTerminatedBy());
	    	setNumberOfLinesToIgnore(old.getNumberOfLinesToIgnore());
    	}
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((getCharsetName() == null) ? 0 : getCharsetName().hashCode());
		result = prime
				* result
				+ ((getFieldsDelimitedBy() == null) ? 0 : getFieldsDelimitedBy()
						.hashCode());
		result = prime * result + getFieldsEnclosedBy();
		result = prime
				* result
				+ ((getLinesTerminatedBy() == null) ? 0 : getLinesTerminatedBy()
						.hashCode());
		result = prime * result + getNumberOfLinesToIgnore();
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
		if (getCharsetName() == null) {
			if (other.getCharsetName() != null) {
				return false;
			}
		} else if (!getCharsetName().equals(other.getCharsetName())) {
			return false;
		}
		if (getFieldsDelimitedBy() == null) {
			if (other.getFieldsDelimitedBy() != null) {
				return false;
			}
		} else if (!getFieldsDelimitedBy().equals(other.getFieldsDelimitedBy())) {
			return false;
		}
		if (getFieldsEnclosedBy() != other.getFieldsEnclosedBy()) {
			return false;
		}
		if (getLinesTerminatedBy() == null) {
			if (other.getLinesTerminatedBy() != null) {
				return false;
			}
		} else if (!getLinesTerminatedBy().equals(other.getLinesTerminatedBy())) {
			return false;
		}
		if (getNumberOfLinesToIgnore() != other.getNumberOfLinesToIgnore()) {
			return false;
		}
		return true;
	}

}
