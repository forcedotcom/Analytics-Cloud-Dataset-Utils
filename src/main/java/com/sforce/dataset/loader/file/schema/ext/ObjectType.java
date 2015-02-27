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
package  com.sforce.dataset.loader.file.schema.ext;

import java.util.List;

public class ObjectType  {	

	 private String name = null; //(Required)
	 private String fullyQualifiedName = null; //(Optional Defaults to name)
	 private String connector = null; //(optional - CSV, SFDC, Workday, etc)
	 private String label = null; //(Optional - defaults to name)
	 private String description = null; //(optional)
	 private String rowLevelSecurityFilter = null; //(Optional)
	 private List<FieldType> fields = null; //(Required)
	 
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFullyQualifiedName() {
		return fullyQualifiedName;
	}
	public void setFullyQualifiedName(String fullyQualifiedName) {
		this.fullyQualifiedName = fullyQualifiedName;
	}
	public String getConnector() {
		return connector;
	}
	public void setConnector(String connector) {
		this.connector = connector;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getRowLevelSecurityFilter() {
		return rowLevelSecurityFilter;
	}
	public void setRowLevelSecurityFilter(String rowLevelSecurityFilter) {
		this.rowLevelSecurityFilter = rowLevelSecurityFilter;
	}
	

	public ObjectType() {
		super();
	}
	 
	public ObjectType(ObjectType old) 
	{
		super();
		if(old!=null)
		{
			setConnector ( old.getConnector());
			setDescription ( old.getDescription());
			setFields(old.getFields());
//			setFields ( new LinkedList<FieldType>());
//			if(old.fields!=null)
//			{
//				for(FieldType obj:old.fields)
//				{
//					setfields.add(new FieldType(obj));
//				}
//			}
			setFullyQualifiedName ( old.getFullyQualifiedName());
			setLabel ( old.getFullyQualifiedName());
			setName ( old.getName());
			setRowLevelSecurityFilter ( old.getRowLevelSecurityFilter());
		
			if(!this.equals(old))
			{
				System.out.println("ObjectType Copy constructor is missing functionality");
			}
		}
	}

	public List<FieldType> getFields() {
		return fields;
	}

	public void setFields(List<FieldType> fields) {
		this.fields = fields;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((getConnector() == null) ? 0 : getConnector().hashCode());
		result = prime * result
				+ ((getDescription() == null) ? 0 : getDescription().hashCode());
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime
				* result
				+ ((getFullyQualifiedName() == null) ? 0 : getFullyQualifiedName()
						.hashCode());
		result = prime * result + ((getLabel() == null) ? 0 : getLabel().hashCode());
		result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
		result = prime
				* result
				+ ((getRowLevelSecurityFilter() == null) ? 0
						: getRowLevelSecurityFilter().hashCode());
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
		ObjectType other = (ObjectType) obj;
		if (getConnector() == null) {
			if (other.getConnector() != null) {
				return false;
			}
		} else if (!getConnector().equals(other.getConnector())) {
			return false;
		}
		if (getDescription() == null) {
			if (other.getDescription() != null) {
				return false;
			}
		} else if (!getDescription().equals(other.getDescription())) {
			return false;
		}
		if (fields == null) {
			if (other.fields != null) {
				return false;
			}
		} else if (!fields.equals(other.fields)) {
			return false;
		}
		if (getFullyQualifiedName() == null) {
			if (other.getFullyQualifiedName() != null) {
				return false;
			}
		} else if (!getFullyQualifiedName().equals(other.getFullyQualifiedName())) {
			return false;
		}
		if (getLabel() == null) {
			if (other.getLabel() != null) {
				return false;
			}
		} else if (!getLabel().equals(other.getLabel())) {
			return false;
		}
		if (getName() == null) {
			if (other.getName() != null) {
				return false;
			}
		} else if (!getName().equals(other.getName())) {
			return false;
		}
		if (getRowLevelSecurityFilter() == null) {
			if (other.getRowLevelSecurityFilter() != null) {
				return false;
			}
		} else if (!getRowLevelSecurityFilter().equals(other.getRowLevelSecurityFilter())) {
			return false;
		}
		return true;
	}

	 
	 
	 

	 
}
