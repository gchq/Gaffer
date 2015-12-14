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
 * A {@link Statistic} that stores a double. When two {@link DoubleCount}s
 * are merged, they are summed.
 */
public class DoubleCount implements Statistic {

	private double count;
	
	public DoubleCount() {
		this.count = 0;
	}
	
	public DoubleCount(double count) {
		this.count = count;
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof DoubleCount) {
			this.count += ((DoubleCount) s).getCount();
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public DoubleCount clone() {
		return new DoubleCount(this.count);
	}
	
	public double getCount() {
		return count;
	}

	public void setCount(double count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "" + count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.count = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.count);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(count);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		DoubleCount other = (DoubleCount) obj;
		if (Double.doubleToLongBits(count) != Double
				.doubleToLongBits(other.count))
			return false;
		return true;
	}

}
