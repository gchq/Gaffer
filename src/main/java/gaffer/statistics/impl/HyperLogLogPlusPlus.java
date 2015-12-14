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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * This {@link Statistic} can be used for estimating the cardinality of a set. An example
 * application of this is to estimate the degree of a node. This statistic is basically a wrapper
 * for an external library.
 * 
 * The external library offers sketches of a range of sizes - the largest of which takes up
 * several GBs of memory, and this is too much to use within tablet servers. A typical use of
 * this sketch would be to estimate the degree of every node in a graph and so there could be
 * a lot of sketches, so even a sketch of size 1KB could be too big. So we choose a very small
 * sketch size here.
 */
public class HyperLogLogPlusPlus implements Statistic {

	private static final long serialVersionUID = -3185005670187056513L;
	private HyperLogLogPlus sketch;

	public HyperLogLogPlusPlus() {
		this.sketch = new HyperLogLogPlus(5, 5);
	}

	/**
	 * Creates a new HyperLogLogPlusPlus and adds all of the provided {@link String}s into the
	 * sketch.
	 * 
	 * @param strings
	 */
	public HyperLogLogPlusPlus(String... strings) {
		this();
		for (String s : strings) {
			add(s);
		}
	}
	
	private HyperLogLogPlusPlus(HyperLogLogPlus hllp) {
		this.sketch = hllp;
	}

	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof HyperLogLogPlusPlus) {
			try {
				sketch.addAll(((HyperLogLogPlusPlus) s).sketch);
			} catch (CardinalityMergeException e) {
				throw new IllegalArgumentException("Exception merging two HyperLogLogPlusPlus Statistics: " + e.getMessage());
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public HyperLogLogPlusPlus clone() {
		try {
			return new HyperLogLogPlusPlus(HyperLogLogPlus.Builder.build(sketch.getBytes()));
		} catch (IOException e) {
			throw new IllegalArgumentException("Exception trying to clone a HyperLogLogPlusPlus Statistic: " + e.getMessage());
		}
	}

	public long getCardinalityEstimate() {
		return sketch.cardinality();
	}

	public void add(Object o) {
		if (o == null) {
			throw new IllegalArgumentException("Can't add a null object to a HyperLogLogPlusPlus");
		}
		sketch.offer(o);
	}
	
	@Override
	public String toString() {
		return "Cardinality estimate: " + getCardinalityEstimate();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int numBytes = in.readInt();
		byte[] bytes = new byte[numBytes];
		for (int i = 0; i < numBytes; i++) {
			bytes[i] = in.readByte();
		}
		this.sketch = HyperLogLogPlus.Builder.build(bytes);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] bytes = sketch.getBytes();
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		HyperLogLogPlusPlus that = (HyperLogLogPlusPlus) o;

		if (sketch != null ? !sketch.equals(that.sketch) : that.sketch != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return sketch != null ? sketch.hashCode() : 0;
	}
}
