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
 * A {@link Statistic} that stores a double. When two {@link DoubleProduct}s
 * are merged, they are multiplied.
 */
public class DoubleProduct implements Statistic {

	private static final long serialVersionUID = 4841962327492322436L;
	private double product;
	
	public DoubleProduct() {
		this.product = 1.0D;
	}
	
	public DoubleProduct(double product) {
		this.product = product;
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof DoubleProduct) {
			this.product *= ((DoubleProduct) s).product;
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public DoubleProduct clone() {
		return new DoubleProduct(this.product);
	}
	
	public double getProduct() {
		return product;
	}

	public void setProduct(double product) {
		this.product = product;
	}

	@Override
	public String toString() {
		return "" + product;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.product = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.product);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(product);
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
		DoubleProduct other = (DoubleProduct) obj;
		if (Double.doubleToLongBits(product) != Double
				.doubleToLongBits(other.product))
			return false;
		return true;
	}

}
