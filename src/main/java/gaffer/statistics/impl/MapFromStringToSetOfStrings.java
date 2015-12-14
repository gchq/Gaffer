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
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;

/**
 * Wraps a map from a string to a sorted set of strings. When two of these are merged,
 * the sets corresponding to the same string are merged. The underlying map is a
 * {@link TreeMap} and the sets are {@link TreeSet}s.
 */
public class MapFromStringToSetOfStrings implements Statistic {

	private static final long serialVersionUID = 4911982748719698575L;
	private SortedMap<String, SortedSet<String>> map;
	
	/**
	 * Default, no-args constructor.
	 */
	public MapFromStringToSetOfStrings() {
		this.map = new TreeMap<String, SortedSet<String>>();
	}
	
	/**
	 * Constructor from an existing {@link Map} of {@link String}s to {@link SortedSet}s of {@link String}s. The
	 * constructor takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapFromStringToSetOfStrings(Map<String, SortedSet<String>> otherMap) {
		this.map = new TreeMap<String, SortedSet<String>>();
		for (String s : otherMap.keySet()) {
			SortedSet<String> value = new TreeSet<String>();
			for (String t : otherMap.get(s)) {
				value.add(t);
			}
			this.map.put(s, value);
		}
	}
	
	/**
	 * Constructor that initialises the internal map with one entry, namely the provided
	 * {@link String} and {@link SortedSet} of {@link String}s.
	 * 
	 * @param string
	 * @param set
	 */
	public MapFromStringToSetOfStrings(String string, SortedSet<String> set) {
		this.map = new TreeMap<String, SortedSet<String>>();
		this.map.put(string, set);
	}

	/**
	 * Adds the provided string key and value into the map - i.e. the value is inserted
	 * into the set corresponding to the key.
	 * 
	 * @param key
	 * @param value
	 */
	public void add(String key, String value) {
		if (!this.map.containsKey(key)) {
			SortedSet<String> set = new TreeSet<String>();
			set.add(value);
			this.map.put(key, set);
		} else {
			this.map.get(key).add(value);
		}
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapFromStringToSetOfStrings) {
			MapFromStringToSetOfStrings otherMap = (MapFromStringToSetOfStrings) s;
			for (String string : otherMap.getMap().keySet()) {
				if (!this.map.containsKey(string)) {
					SortedSet<String> set = new TreeSet<String>();
					set.addAll(otherMap.map.get(string));
					this.map.put(string, set);
				} else {
					this.map.get(string).addAll(otherMap.map.get(string));
				}
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public MapFromStringToSetOfStrings clone() {
		return new MapFromStringToSetOfStrings(this.map);
	}
	
	/**
	 * Returns the count for the provided string. It returns null if
	 * there is no value associated to the string.
	 * 
	 * @param s
	 * @return
	 */
	public SortedSet<String> getSet(String s) {
		return map.get(s);
	}
	
	/**
	 * Returns the map of strings to counts.
	 * 
	 * @return
	 */
	public Map<String, SortedSet<String>> getMap() {
		return map;
	}

	@Override
	public String toString() {
		return map.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int num = in.readInt();
		map.clear();
		for (int i = 0; i < num; i++) {
			String key = Text.readString(in);
			int size = in.readInt();
			SortedSet<String> set = new TreeSet<String>();
			for (int j = 0; j < size; j++) {
				set.add(Text.readString(in));
			}
			map.put(key, set);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (String s : map.keySet()) {
			Text.writeString(out, s);
			int setSize = map.get(s).size();
			out.writeInt(setSize);
			for (String t : map.get(s)) {
				Text.writeString(out, t);
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((map == null) ? 0 : map.hashCode());
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
		MapFromStringToSetOfStrings other = (MapFromStringToSetOfStrings) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
