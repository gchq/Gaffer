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
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * A {@link Statistic} that records a list of integers. When two of these are merged, the deduped
 * list is formed.
 */
public class IntSet implements Statistic {

	private static final long serialVersionUID = 679833011459803354L;
	private RoaringBitmap bitmap;

	public IntSet() {
		bitmap = new RoaringBitmap();
	}

	public IntSet(Set<Integer> other) {
		this();
		for (int i : other) {
			add(i);
		}
	}

	public IntSet(int... ints) {
		this();
		add(ints);
	}

	public void add(int... ints) {
		for (int i : ints) {
			bitmap.add(i);
		}
	}

	public RoaringBitmap getRoaringBitmap() {
		return bitmap;
	}

	public Iterator<Integer> getIterator() {
		return bitmap.iterator();
	}

	/**
	 * Indicates whether the supplied int is in the set.
	 *
	 * @param i
	 * @return
	 */
	public boolean contains(int i) {
		return bitmap.contains(i);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Integer> it = bitmap.iterator();
		while (it.hasNext()) {
			if (sb.length() > 0) {
				sb.append(" ");
			}
			sb.append(it.next());
		}
		return sb.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		bitmap.serialize(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		bitmap.deserialize(in);
	}

	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof IntSet) {
			IntSet is = (IntSet) s;
			bitmap.or(is.bitmap);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public IntSet clone() {
		IntSet is = new IntSet();
		is.bitmap.or(this.bitmap);
		return is;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bitmap == null) ? 0 : bitmap.hashCode());
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
		IntSet other = (IntSet) obj;
		if (bitmap == null) {
			if (other.bitmap != null)
				return false;
		} else if (!bitmap.equals(other.bitmap))
			return false;
		return true;
	}
	
}
