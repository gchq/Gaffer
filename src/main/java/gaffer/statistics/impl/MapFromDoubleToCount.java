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
 * Wraps a map from doubles to integers. When two of these are merged, the values
 * corresponding to the same key are summed.
 * 
 * The map is a sorted map, so that the contents can be iterated through in
 * increasing order of the key.
 */
public class MapFromDoubleToCount implements Statistic {

	private static final long serialVersionUID = -1254371748089441745L;
	private TreeMap<Double, Integer> map;
	
	/**
	 * Default, no-args constructor.
	 */
	public MapFromDoubleToCount() {
		this.map = new TreeMap<Double, Integer>();
	}
	
	/**
	 * Constructor from an existing {@link Map} of {@link Integer}s to {@link Integer}s. The constructor
	 * takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapFromDoubleToCount(Map<Double, Integer> otherMap) {
		this.map = new TreeMap<Double, Integer>();
		for (Double i : otherMap.keySet()) {
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
	public MapFromDoubleToCount(double key, int count) {
		this.map = new TreeMap<Double, Integer>();
		this.map.put(key, count);
	}
	
	/**
	 * Adds the provided double and count into the map. If the key is already in the map
	 * then the counts will be added.
	 * 
	 * @param key
	 * @param count
	 */
	public void add(double key, int count) {
		if (!this.map.containsKey(key)) {
			this.map.put(key, count);
		} else {
			this.map.put(key, new Integer(this.map.get(key) + count));
		}
	}
	
	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapFromDoubleToCount) {
			MapFromDoubleToCount otherMap = (MapFromDoubleToCount) s;
			for (Double i : otherMap.getMap().keySet()) {
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
	public MapFromDoubleToCount clone() {
		return new MapFromDoubleToCount(this.map);
	}
	
	/**
	 * Returns the count for the provided double. It returns null if
	 * there is no value associated to the string.
	 * 
	 * @param i
	 * @return
	 */
	public int getCount(double i) {
		return map.get(i);
	}
	
	/**
	 * Returns the map of doubles to counts.
	 * 
	 * @return
	 */
	public Map<Double, Integer> getMap() {
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
			double j = in.readDouble();
			this.map.put(j, in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (double i : map.keySet()) {
			out.writeDouble(i);
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
		MapFromDoubleToCount other = (MapFromDoubleToCount) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
