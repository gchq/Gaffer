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

/**
 * A {@link Statistic} that stores a long. When two {@link LongCount}s
 * are merged, they are simply summed.
 */
public class LongCount implements Statistic {

	private static final long serialVersionUID = -4331846671989445124L;
	private long count;
	
	public LongCount() {
		this.count = 0;
	}
	
	public LongCount(long count) {
		this.count = count;
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof LongCount) {
			this.count += ((LongCount) s).getCount();
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public LongCount clone() {
		return new LongCount(this.count);
	}
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "" + count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.count);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
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
		LongCount other = (LongCount) obj;
		if (count != other.count)
			return false;
		return true;
	}

}
