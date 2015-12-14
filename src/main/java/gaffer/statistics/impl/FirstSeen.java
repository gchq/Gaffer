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
 * A {@link Statistic} that stores a {@link Date}. When two {@link FirstSeen}s
 * are merged, the earliest is returned.
 */
public class FirstSeen implements Statistic {

	private static final long serialVersionUID = 3816239194798414227L;
	private Date firstSeen = new Date();
	
	public FirstSeen() {
		
	}
	
	public FirstSeen(Date firstSeen) {
		this.firstSeen = (Date) firstSeen.clone();
	}

	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof FirstSeen) {
			// Take the earliest Date.
			// If this firstSeen is after s then set this one to s.
			if ( this.firstSeen.after(((FirstSeen) s).getFirstSeen()) ) {
				this.firstSeen.setTime(((FirstSeen) s).getFirstSeen().getTime());
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}
	
	@Override
	public FirstSeen clone() {
		return new FirstSeen(this.firstSeen);
	}
	
	public void readFields(DataInput in) throws IOException {
		firstSeen.setTime(in.readLong());
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(firstSeen.getTime());
	}

	public Date getFirstSeen() {
		return firstSeen;
	}

	public void setFirstSeen(Date firstSeen) {
		this.firstSeen = firstSeen;
	}

	@Override
	public String toString() {
		return "" + firstSeen.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((firstSeen == null) ? 0 : firstSeen.hashCode());
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
		FirstSeen other = (FirstSeen) obj;
		if (firstSeen == null) {
			if (other.firstSeen != null)
				return false;
		} else if (!firstSeen.equals(other.firstSeen))
			return false;
		return true;
	}
	
}
