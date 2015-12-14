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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import gaffer.statistics.Statistic;

/**
 * An {@link Statistic} that stores a single double. When two statistics of this
 * type are merged, the largest value is retained.
 * 
 * Note: Uses floating-point equality, so one should not depend on two (@link
 * MaxDouble}s constructed from different series of arithmetic operations
 * evaluating as equal.
 */
public class MaxDouble implements Statistic {

	private static final long serialVersionUID = 7478238531382397030L;
	private double max;

	public MaxDouble() { }

	public MaxDouble(double max) {
		this.max = max;
	}
	
	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public MaxDouble clone() {
		return new MaxDouble(max);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		max = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(max);
	}

	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MaxDouble) {
			max = Math.max(max, ((MaxDouble) s).max);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type "
					+ s.getClass() + " with a " + this.getClass() + ".");
		}
	}

	@Override
	public String toString() {
		return "" + max;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(max);
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
		MaxDouble other = (MaxDouble) obj;
		if (Double.doubleToLongBits(max) != Double.doubleToLongBits(other.max))
			return false;
		return true;
	}

}
