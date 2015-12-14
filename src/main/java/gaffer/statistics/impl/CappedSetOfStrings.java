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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;

/**
 * A {@link Statistic} that contains a set of at most n strings, where n is specified by the
 * creator of the {@link CappedSetOfStrings}. If the set already contains n strings and another
 * is added to it, then the set is cleared and a flag is set to indicate that the set has gone beyond
 * the maximum capacity. When two of these are merged, the sets are merged - if either are full then
 * the merged set is full.
 */
public class CappedSetOfStrings implements Statistic {

	private static final long serialVersionUID = -8971142341182660487L;
	private Set<String> set = null;
	private int maxSize;
	private boolean full;
	
	public CappedSetOfStrings() {
		this.set = new HashSet<String>();
		this.maxSize = Integer.MAX_VALUE;
		this.full = false;
	}
	
	public CappedSetOfStrings(int maxSize) {
		if (maxSize < 1) {
			throw new IllegalArgumentException("Can't create a CappedSetOfStrings of size less than 1");
		}
		this.set = new HashSet<String>();
		this.maxSize = maxSize;
		this.full = false;
	}
	
	public CappedSetOfStrings(int maxSize, Set<String> other) {
		this(maxSize);
		if (other.size() > maxSize) {
			throw new IllegalArgumentException("Creating a CappedSetOfStrings of maximum size " + maxSize
					+ " with initial set of size " + other.size());
		}
		addStrings(other);
	}
	
	public CappedSetOfStrings(int maxSize, boolean full, Set<String> other) {
		this(maxSize);
		this.full = full;
		if (other.size() > maxSize) {
			throw new IllegalArgumentException("Creating a CappedSetOfStrings of maximum size " + maxSize
					+ " with initial set of size " + other.size());
		}
		if (other.size() != 0 && full) {
			throw new IllegalArgumentException("Creating a CappedSetOfStrings of maximum size " + maxSize
					+ " with initial set of size " + other.size() + " and marked as full - initial set should be empty");
		}
		addStrings(other);
	}
	
	/**
	 * Merges another {@link CappedSetOfStrings} into this one. This is only permitted if
	 * the other one has the same maxSize as this one.
	 */
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof CappedSetOfStrings) {
			CappedSetOfStrings other = (CappedSetOfStrings) s;
			// Can only merge two CappedSetOfStrings with the same maxSize.
			if (maxSize != other.maxSize) {
				throw new IllegalArgumentException("Can't merge a CappedSetOfStrings with maxSize of "
						+ other.maxSize + " with this one of maxSize " + maxSize);
			}
			// If this set is already full, or the other set is full, then the merged set must be full.
			if (full || other.full) {
				full = true;
				set.clear(); // Need to do this if other is full.
				return;
			}
			// Otherwise add all of the items in the other set.
			addStrings(other.getSet());
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	/**
	 * Adds a string to the set. If this causes the size of the set to become greater than
	 * {@link #maxSize} then the set will be cleared and this object will be marked as
	 * being full. If this set is already full then adding a string has no effect.
	 * 
	 * @param s
	 */
	public void addString(String s) {
		if (full) {
			return;
		}
		set.add(s);
		if (set.size() > maxSize) {
			set.clear();
			full = true;
		}
	}
	
	/**
	 * Adds all the strings to the set. If this causes the size of the set to become greater than
	 * {@link #maxSize} then the set will be cleared and this object will be marked as
	 * being full. If this set is already full then adding these strings has no effect.
	 * 
	 * @param strings
	 */
	public void addStrings(Set<String> strings) {
		for (String s : strings) {
			addString(s);
			if (full) {
				return;
			}
		}
	}
	
	/**
	 * Removes the provided {@link String} from the set. If the string is not in
	 * the set then this has no effect. If the set is full then removing a string has
	 * no effect (as the set is actually empty, and there is a flag to indicate that
	 * it is full).
	 * 
	 * @param s
	 */
	public void removeString(String s) {
		set.remove(s);
	}
	
	/**
	 * Removes the provided {@link String}s from the set. If the set is full then
	 * removing these strings has no effect (as the set is actually empty, and there is
	 * a flag to indicate that it is full).
	 * 
	 * @param strings
	 */
	public void removeStrings(Set<String> strings) {
		set.removeAll(strings);
	}
	
	@Override
	public CappedSetOfStrings clone() {
		return new CappedSetOfStrings(maxSize, full, this.set);
	}
	
	/**
	 * Returns true if the set is full.
	 * 
	 * @return
	 */
	public boolean isFull() {
		return full;
	}
	
	/**
	 * Returns the set of {@link String}s.
	 * 
	 * @return
	 */
	public Set<String> getSet() {
		return set;
	}

	/**
	 * Returns the size of the set. If the set is full then 0 will be returned, so
	 * this method should be used with {@link #isFull()}.
	 * 
	 * @return
	 */
	public int getSize() {
		return set.size();
	}
	
	/**
	 * Returns the maximum size that the set can be.
	 * 
	 * @return
	 */
	public int getMaxSize() {
		return maxSize;
	}
	
	/**
	 * Removes all entries from the set and set full to be false.
	 */
	public void clear() {
		set.clear();
		full = false;
	}
	
	@Override
	public String toString() {
		if (full) {
			return ">" + maxSize;
		}
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String s : set) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(s);
			i++;
		}
		return sb.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clear();
		maxSize = in.readInt();
		full = in.readBoolean();
		if (!full) {
			int setSize = in.readInt();
			for (int i = 0; i < setSize; i++) {
				set.add(Text.readString(in)); // Can add directly to set and bypass size checks as there can't be more strings than maxSize.
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(maxSize);
		out.writeBoolean(full);
		if (!full) {
			out.writeInt(set.size());
			for (String s : set) {
				Text.writeString(out, s);
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (full ? 1231 : 1237);
		result = prime * result + maxSize;
		result = prime * result + ((set == null) ? 0 : set.hashCode());
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
		CappedSetOfStrings other = (CappedSetOfStrings) obj;
		if (full != other.full)
			return false;
		if (maxSize != other.maxSize)
			return false;
		if (set == null) {
			if (other.set != null)
				return false;
		} else if (!set.equals(other.set))
			return false;
		return true;
	}

}
