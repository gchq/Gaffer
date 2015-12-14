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
 * A {@link Statistic} that stores a long. When two {@link LongMax}s
 * are merged, the result is the maximum of the two longs.
 */
public class LongMax implements Statistic {

	private static final long serialVersionUID = 5684109936361962749L;
	private long max;
	
	public LongMax() {
		this.max = Long.MIN_VALUE;
	}
	
	public LongMax(long max) {
		this.max = max;
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof LongMax) {
			this.max = Math.max(max, ((LongMax) s).max);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public LongMax clone() {
		return new LongMax(this.max);
	}
	
	public long getMax() {
		return max;
	}

	public void setMax(long max) {
		this.max = max;
	}

	@Override
	public String toString() {
		return "" + max;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.max = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.max);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (max ^ (max >>> 32));
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
		LongMax other = (LongMax) obj;
		if (max != other.max)
			return false;
		return true;
	}

}
