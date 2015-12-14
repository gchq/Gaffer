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
 * A {@link Statistic} that stores a long. When two {@link LongMin}s
 * are merged, the result is the minimum of the two longs.
 */
public class LongMin implements Statistic {

	private static final long serialVersionUID = -4182767611113930599L;
	private long min;
	
	public LongMin() {
		this.min = Long.MAX_VALUE;
	}
	
	public LongMin(long max) {
		this.min = max;
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof LongMin) {
			this.min = Math.min(min, ((LongMin) s).min);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public LongMin clone() {
		return new LongMin(this.min);
	}
	
	public long getMin() {
		return min;
	}

	public void setMin(long min) {
		this.min = min;
	}

	@Override
	public String toString() {
		return "" + min;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.min = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.min);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (min ^ (min >>> 32));
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
		LongMin other = (LongMin) obj;
		if (min != other.min)
			return false;
		return true;
	}

}
