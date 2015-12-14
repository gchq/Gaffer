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
 * A {@link Statistic} that stores a double. When two {@link DoubleProductViaLogs}s
 * are merged, they are multiplied.
 */
public class DoubleProductViaLogs implements Statistic {

	private static final long serialVersionUID = 7945724238695551665L;
	private double logProduct;
	
	public DoubleProductViaLogs() {
		this.logProduct = 0.0D;
	}
	
	public DoubleProductViaLogs(double product) {
		this.logProduct = Math.log(product);
	}
	
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof DoubleProductViaLogs) {
			this.logProduct += ((DoubleProductViaLogs) s).logProduct;
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public DoubleProductViaLogs clone() {
		return new DoubleProductViaLogs(this.logProduct);
	}
	
	public double getProduct() {
		return Math.exp(logProduct);
	}

	public double getLogProduct() {
		return logProduct;
	}
	
	public void setProduct(double product) {
		this.logProduct = Math.log(product);
	}
	
	public void setLogProduct(double logProduct) {
		this.logProduct = logProduct;
	}
	
	@Override
	public String toString() {
		return "" + getProduct();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.logProduct = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.logProduct);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(logProduct);
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
		DoubleProductViaLogs other = (DoubleProductViaLogs) obj;
		if (Double.doubleToLongBits(logProduct) != Double
				.doubleToLongBits(other.logProduct))
			return false;
		return true;
	}

}
