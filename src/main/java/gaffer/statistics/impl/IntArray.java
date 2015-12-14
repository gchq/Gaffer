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
import java.util.Arrays;

import gaffer.statistics.Statistic;

/**
 * Wraps an array of integers. When two of these are merged, the counts are
 * simply added. If the arrays are of different lengths, an {@link IllegalArgumentException}
 * is thrown.
 * 
 * A typical use of this is to store the profile of activity seen across different hours of
 * the day.
 */
public class IntArray implements Statistic {

	private static final long serialVersionUID = 751141416378054244L;
	private int[] array;
	
	public IntArray() {}
	
	public IntArray(int arraySize) {
		this.array = new int[arraySize];
	}
	
	public IntArray(int[] array) {
		this.array = array;
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof IntArray) {
			int[] arrayToMergeIn = ((IntArray) s).getArray();
			if (array.length != arrayToMergeIn.length) {
				throw new IllegalArgumentException("Trying to merge an IntArray of length " + array.length
						+ " with one of length " + arrayToMergeIn.length);
			}
			for (int i = 0; i < array.length; i++) {
				array[i] += arrayToMergeIn[i];
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public IntArray clone() {
		return new IntArray(this.array);
	}
	
	public int[] getArray() {
		return array;
	}

	public void setArray(int[] array) {
		this.array = array;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (int i = 0; i < array.length; i++) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(array[i]);
		}
		sb.append(']');
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int arrayLength = in.readInt();
		this.array = new int[arrayLength];
		boolean sparse = in.readBoolean();
		if (sparse) {
			int numToRead = in.readInt();
			for (int i = 0; i < numToRead; i++) {
				this.array[in.readInt()] = in.readInt();
			}
		} else {
			for (int i = 0; i < arrayLength; i++) {
				this.array[i] = in.readInt();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.array.length);
		int nonZeroCount = writeAsSparseArray();
		if (nonZeroCount != -1) {
			out.writeBoolean(true); // Indicates that writing in sparse form
			out.writeInt(nonZeroCount); // Number of non-zero counts
			for (int i = 0; i < this.array.length; i++) {
				if (this.array[i] != 0) {
					out.writeInt(i);
					out.writeInt(this.array[i]);
				}
			}
		} else {
			out.writeBoolean(false);
			for (int i = 0; i < this.array.length; i++) {
				out.writeInt(this.array[i]);
			}
		}
	}

	private int writeAsSparseArray() {
		// Count number of non-zero entries
		int count = 0;
		for (int i = 0; i < this.array.length; i++) {
			if (this.array[i] != 0) {
				count++;
			}
		}
		// To store non-sparse we need (array.length * 4) bytes. To store sparse
		// we need (4 + num_non_zero_entries * 2 * 4) bytes. (Both these figures ignore
		// the bytes to record the length of the array and the byte to indicate
		// whether it is stored sparsely or not.) So need num_non_zero_entries to
		// be less than half the (length of the array - 1) for it to be best to store
		// it sparsely.
		if (count < ((this.array.length - 1.0) / 2.0)) {
			return count;
		}
		return -1;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(array);
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
		IntArray other = (IntArray) obj;
		if (!Arrays.equals(array, other.array))
			return false;
		return true;
	}
	
}
