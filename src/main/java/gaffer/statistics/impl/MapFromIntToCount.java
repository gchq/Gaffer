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
 * Wraps a map from integers to integers. When two of these are merged, the values
 * corresponding to the same integer key are summed.
 * 
 * The map is a sorted map, so that the contents can be iterated through in
 * increasing order of the key.
 */
public class MapFromIntToCount implements Statistic {

	private static final long serialVersionUID = -6551869492430216009L;
	private TreeMap<Integer, Integer> map;
	
	/**
	 * Default, no-args constructor.
	 */
	public MapFromIntToCount() {
		this.map = new TreeMap<Integer, Integer>();
	}
	
	/**
	 * Constructor from an existing {@link Map} of {@link Integer}s to {@link Integer}s. The constructor
	 * takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapFromIntToCount(Map<Integer, Integer> otherMap) {
		this.map = new TreeMap<Integer, Integer>();
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
	public MapFromIntToCount(int key, int count) {
		this.map = new TreeMap<Integer, Integer>();
		this.map.put(key, count);
	}
	
	/**
	 * Adds the provided int and count into the map.
	 * 
	 * @param key
	 * @param count
	 */
	public void add(int key, int count) {
		if (!this.map.containsKey(key)) {
			this.map.put(key, count);
		} else {
			this.map.put(key, new Integer(this.map.get(key) + count));
		}
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapFromIntToCount) {
			MapFromIntToCount otherMap = (MapFromIntToCount) s;
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
	public MapFromIntToCount clone() {
		return new MapFromIntToCount(this.map);
	}
	
	/**
	 * Returns the count for the provided integer. It returns null if
	 * there is no value associated to the string.
	 * 
	 * @param i
	 * @return
	 */
	public int getCount(int i) {
		return map.get(i);
	}
	
	/**
	 * Returns the map of integers to counts.
	 * 
	 * @return
	 */
	public Map<Integer, Integer> getMap() {
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
			this.map.put(j, in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (int i : map.keySet()) {
			out.writeInt(i);
			out.writeInt(map.get(i));
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
		MapFromIntToCount other = (MapFromIntToCount) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
