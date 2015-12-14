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
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/**
 * Wraps a map from string to integers. When two of these are merged, the integers
 * corresponding to the same string are summed.
 * 
 * A typical use of this is to create a timeseries where the string represents the
 * time interval and the int measures the activity level during that period. When
 * maps corresponding to different time periods are merged, the result is the
 * timeseries corresponding to the total time window.
 */
public class MapOfCounts implements Statistic {

	private static final long serialVersionUID = 3331554356600208122L;
	private Map<String, Integer> map;
	
	/**
	 * Default, no-args constructor.
	 */
	public MapOfCounts() {
		this.map = new TreeMap<String, Integer>();
	}
	
	/**
	 * Constructor from an existing {@link Map} of {@link String}s to {@link Integer}s. The constructor
	 * takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapOfCounts(Map<String, Integer> otherMap) {
		this.map = new TreeMap<String, Integer>();
		for (String s : otherMap.keySet()) {
			this.map.put(s, otherMap.get(s));
		}
	}
	
	/**
	 * Constructor that initialises the internal map with one entry, namely the provided
	 * {@link String} and {@link Integer}.
	 * 
	 * @param string
	 * @param count
	 */
	public MapOfCounts(String string, int count) {
		this.map = new TreeMap<String, Integer>();
		this.map.put(string, count);
	}

	/**
	 * Adds the provided string and count into the map.
	 * 
	 * @param string
	 * @param count
	 */
	public void add(String string, int count) {
		if (!this.map.containsKey(string)) {
			this.map.put(string, count);
		} else {
			this.map.put(string, new Integer(this.map.get(string) + count));
		}
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapOfCounts) {
			MapOfCounts otherMap = (MapOfCounts) s;
			for (String string : otherMap.getMap().keySet()) {
				if (!this.map.containsKey(string)) {
					this.map.put(string, otherMap.getMap().get(string));
				} else {
					this.map.put(string, this.map.get(string) + otherMap.getMap().get(string));
				}
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public MapOfCounts clone() {
		return new MapOfCounts(this.map);
	}
	
	/**
	 * Returns the count for the provided string. It returns null if
	 * there is no value associated to the string.
	 * 
	 * @param s
	 * @return
	 */
	public int getCount(String s) {
		return map.get(s);
	}
	
	/**
	 * Returns the map of strings to counts.
	 * 
	 * @return
	 */
	public Map<String, Integer> getMap() {
		return map;
	}

	@Override
	public String toString() {
		return map.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int num = in.readInt();
		this.map.clear();
		for (int i = 0; i < num; i++) {
			String s = Text.readString(in);
			this.map.put(s, in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (String s : map.keySet()) {
			Text.writeString(out, s);
			out.writeInt(map.get(s));
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
		MapOfCounts other = (MapOfCounts) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
