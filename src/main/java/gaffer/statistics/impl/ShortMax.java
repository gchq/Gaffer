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
 * A {@link Statistic} that stores a short. When two {@link ShortMax}s
 * are merged, the result is the maximum of the two shorts.
 */
public class ShortMax implements Statistic {

	private static final long serialVersionUID = -6921213796720126474L;
	private short max;
	
	public ShortMax() {
		this.max = Short.MIN_VALUE;
	}
	
	public ShortMax(short max) {
		this.max = max;
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof ShortMax) {
			this.max = (short) Math.max(max, ((ShortMax) s).max);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public ShortMax clone() {
		return new ShortMax(this.max);
	}
	
	public short getMax() {
		return max;
	}

	public void setMax(short max) {
		this.max = max;
	}

	@Override
	public String toString() {
		return "" + max;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.max = in.readShort();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(this.max);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + max;
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
		ShortMax other = (ShortMax) obj;
		if (max != other.max)
			return false;
		return true;
	}
	
}
