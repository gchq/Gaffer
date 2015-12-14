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
package gaffer.statistics.impl;

import gaffer.statistics.Statistic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * A {@link Statistic} that stores a {@link Date}. When two {@link LastSeen}s
 * are merged, the latest is returned.
 */
public class LastSeen implements Statistic {

	private static final long serialVersionUID = -7062541499473571215L;
	private Date lastSeen = new Date();
	
	public LastSeen() {
		
	}
	
	public LastSeen(Date lastSeen) {
		this.lastSeen = (Date) lastSeen.clone();
	}

	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof LastSeen) {
			// Take the latest Date.
			// If this lastSeen is before s then set this one to s.
			if ( this.lastSeen.before(((LastSeen) s).getLastSeen()) ) {
				this.lastSeen.setTime(((LastSeen) s).getLastSeen().getTime());
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}
	
	@Override
	public LastSeen clone() {
		return new LastSeen(this.lastSeen);
	}
	
	public void readFields(DataInput in) throws IOException {
		lastSeen.setTime(in.readLong());
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(lastSeen.getTime());
	}

	public Date getLastSeen() {
		return lastSeen;
	}

	public void setLastSeen(Date lastSeen) {
		this.lastSeen = lastSeen;
	}

	@Override
	public String toString() {
		return "" + lastSeen.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((lastSeen == null) ? 0 : lastSeen.hashCode());
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
		LastSeen other = (LastSeen) obj;
		if (lastSeen == null) {
			if (other.lastSeen != null)
				return false;
		} else if (!lastSeen.equals(other.lastSeen))
			return false;
		return true;
	}
	
}
