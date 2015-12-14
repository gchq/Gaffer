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

/**
 * Wraps a map from integers to longs. When two of these are merged, the values
 * corresponding to the same integer key are summed.
 * 
 * The map is a sorted map, so that the contents can be iterated through in
 * increasing order of the key.
 */
public class MapFromIntToLongCount implements Statistic {

	private static final long serialVersionUID = -6121762233149053933L;
	private TreeMap<Integer, Long> map;
	
	/**
	 * Default, no-args constructor.
	 */
	public MapFromIntToLongCount() {
		this.map = new TreeMap<Integer, Long>();
	}
	
	/**
	 * Constructor from an existing {@link Map} of {@link Integer}s to {@link Integer}s. The constructor
	 * takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapFromIntToLongCount(Map<Integer, Long> otherMap) {
		this.map = new TreeMap<Integer, Long>();
		for (Integer i : otherMap.keySet()) {
			this.map.put(i, otherMap.get(i));
		}
	}
	
	/**
	 * Constructor that initialises the internal map with one entry, namely the provided
	 * key and count.
	 * 
	 * @param key
	 * @param count
	 */
	public MapFromIntToLongCount(int key, long count) {
		this.map = new TreeMap<Integer, Long>();
		this.map.put(key, count);
	}
	
	/**
	 * Adds the provided long and count into the map.
	 * 
	 * @param key
	 * @param count
	 */
	public void add(int key, long count) {
		if (!this.map.containsKey(key)) {
			this.map.put(key, count);
		} else {
			this.map.put(key, new Long(this.map.get(key) + count));
		}
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapFromIntToLongCount) {
			MapFromIntToLongCount otherMap = (MapFromIntToLongCount) s;
			for (Integer i : otherMap.getMap().keySet()) {
				if (!this.map.containsKey(i)) {
					this.map.put(i, otherMap.getMap().get(i));
				} else {
					this.map.put(i, this.map.get(i) + otherMap.getMap().get(i));
				}
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public MapFromIntToLongCount clone() {
		return new MapFromIntToLongCount(this.map);
	}
	
	/**
	 * Returns the count for the provided integer. It returns null if
	 * there is no value associated to the string.
	 * 
	 * @param i
	 * @return
	 */
	public long getCount(int i) {
		return map.get(i);
	}
	
	/**
	 * Returns the map of integers to counts.
	 * 
	 * @return
	 */
	public Map<Integer, Long> getMap() {
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
			int j = in.readInt();
			this.map.put(j, in.readLong());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (int i : map.keySet()) {
			out.writeInt(i);
			out.writeLong(map.get(i));
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
		MapFromIntToLongCount other = (MapFromIntToLongCount) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
