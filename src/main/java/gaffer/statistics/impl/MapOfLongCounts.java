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
 * Wraps a map from string to longs. When two of these are merged, the longs
 * corresponding to the same string are summed.
 */
public class MapOfLongCounts implements Statistic {

	private static final long serialVersionUID = 4054192384517766131L;
	private Map<String, Long> map;
	
	/**
	 * Default, no-args constructor.
	 */
	public MapOfLongCounts() {
		this.map = new TreeMap<String, Long>();
	}
	
	/**
	 * Constructor from an existing {@link Map} of {@link String}s to {@link Long}s. The constructor
	 * takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapOfLongCounts(Map<String, Long> otherMap) {
		this.map = new TreeMap<String, Long>();
		for (String s : otherMap.keySet()) {
			this.map.put(s, otherMap.get(s));
		}
	}
	
	/**
	 * Constructor that initialises the map with one entry, namely the provided
	 * {@link String} and {@link Long}.
	 * 
	 * @param string
	 * @param count
	 */
	public MapOfLongCounts(String string, long count) {
		this.map = new TreeMap<String, Long>();
		this.map.put(string, count);
	}

	/**
	 * Adds the provided string and count into the map.
	 * 
	 * @param string
	 * @param count
	 */
	public void add(String string, long count) {
		if (!this.map.containsKey(string)) {
			this.map.put(string, count);
		} else {
			this.map.put(string, new Long(this.map.get(string) + count));
		}
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapOfLongCounts) {
			MapOfLongCounts otherMap = (MapOfLongCounts) s;
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
	public MapOfLongCounts clone() {
		return new MapOfLongCounts(this.map);
	}
	
	/**
	 * Returns the count for the provided string. It returns null if
	 * there is no value associated to the string.
	 * 
	 * @param s
	 * @return
	 */
	public long getCount(String s) {
		return map.get(s);
	}
	
	/**
	 * Returns the map of strings to longs.
	 * 
	 * @return
	 */
	public Map<String, Long> getMap() {
		return map;
	}

	@Override
	public String toString() {
		return map.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		long num = in.readInt();
		this.map.clear();
		for (int i = 0; i < num; i++) {
			String s = Text.readString(in);
			this.map.put(s, in.readLong());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (String s : map.keySet()) {
			Text.writeString(out, s);
			out.writeLong(map.get(s));
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
		MapOfLongCounts other = (MapOfLongCounts) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
