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
 * A simple {@link Statistic} that contains a set of strings. When two of these are merged,
 * the sets are merged.
 */
public class SetOfStrings implements Statistic {

	private static final long serialVersionUID = -1476567738515282646L;
	private Set<String> set = null;
	
	public SetOfStrings() {
		set = new HashSet<String>();
	}
	
	public SetOfStrings(Set<String> other) {
		this();
		set.addAll(other);
	}

	public SetOfStrings(String... strings) {
		this();
		for (String s : strings) {
			addString(s);
		}
	}

	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof SetOfStrings) {
			set.addAll(((SetOfStrings) s).getSet());
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	public void addString(String s) {
		set.add(s);
	}
	
	@Override
	public SetOfStrings clone() {
		return new SetOfStrings(this.set);
	}
	
	public Set<String> getSet() {
		return set;
	}

	/**
	 * Removes all entries from this set.
	 */
	public void clear() {
		set.clear();
	}
	
	@Override
	public String toString() {
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
		int setSize = in.readInt();
		for (int i = 0; i < setSize; i++) {
			set.add(Text.readString(in));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.set.size());
		for (String s : this.set) {
			Text.writeString(out, s);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		SetOfStrings other = (SetOfStrings) obj;
		if (set == null) {
			if (other.set != null)
				return false;
		} else if (!set.equals(other.set))
			return false;
		return true;
	}

}
