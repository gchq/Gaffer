/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.graph;

import java.io.Serializable;

/**
 * Represents a range of {@link TypeValue}s.
 */
public class TypeValueRange implements Serializable {

	private static final long serialVersionUID = -2619836362330469735L;
	private String startType;
	private String startValue;
	private String endType;
	private String endValue;
	
	public TypeValueRange(String startType, String startValue, String endType, String endValue) {
		this.startType = startType;
		this.startValue = startValue;
		this.endType = endType;
		this.endValue = endValue;
	}

	public String getStartType() {
		return startType;
	}

	public void setStartType(String startType) {
		this.startType = startType;
	}

	public String getStartValue() {
		return startValue;
	}

	public void setStartValue(String startValue) {
		this.startValue = startValue;
	}

	public String getEndType() {
		return endType;
	}

	public void setEndType(String endType) {
		this.endType = endType;
	}

	public String getEndValue() {
		return endValue;
	}

	public void setEndValue(String endValue) {
		this.endValue = endValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endType == null) ? 0 : endType.hashCode());
		result = prime * result
				+ ((endValue == null) ? 0 : endValue.hashCode());
		result = prime * result
				+ ((startType == null) ? 0 : startType.hashCode());
		result = prime * result
				+ ((startValue == null) ? 0 : startValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TypeValueRange other = (TypeValueRange) obj;
		if (endType == null) {
			if (other.endType != null)
				return false;
		} else if (!endType.equals(other.endType))
			return false;
		if (endValue == null) {
			if (other.endValue != null)
				return false;
		} else if (!endValue.equals(other.endValue))
			return false;
		if (startType == null) {
			if (other.startType != null)
				return false;
		} else if (!startType.equals(other.startType))
			return false;
		if (startValue == null) {
			if (other.startValue != null)
				return false;
		} else if (!startValue.equals(other.startValue))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TypeValueRange [startType=" + startType + ", startValue="
				+ startValue + ", endType=" + endType + ", endValue="
				+ endValue + "]";
	}
}
