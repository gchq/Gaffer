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
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/**
 * Wraps a map from {@link String}s to {@link MinuteBitMap}s. When two of these
 * are merged, the {@link MinuteBitMap}s corresponding to the same string are
 * merged in turn.
 */
public class MapOfMinuteBitMaps implements Statistic {

	private static final long serialVersionUID = -3877401661942592152L;
	private Map<String, MinuteBitMap> map;

	/**
	 * Default, no-args constructor.
	 */
	public MapOfMinuteBitMaps() {
		this.map = new TreeMap<String, MinuteBitMap>();
	}

	/**
	 * Constructor from an existing {@link Map} of {@link String}s to {@link MinuteBitMap}s. The
	 * constructor takes a deep copy of the map.
	 * 
	 * @param otherMap
	 */
	public MapOfMinuteBitMaps(Map<String, MinuteBitMap> otherMap) {
		this();
		for (String s : otherMap.keySet()) {
			this.map.put(s, otherMap.get(s).clone());
		}
	}

	/**
	 * Constructor from a single key-value pair.
	 * 
	 * @param key
	 * @param value
	 */
	public MapOfMinuteBitMaps(String key, MinuteBitMap value) {
		this();
		this.add(key, value);
	}

	/**
	 * Constructor from a single key-value pair.
	 * 
	 * @param key
	 * @param value
	 */
	public MapOfMinuteBitMaps(String key, Date value) {
		this();
		this.add(key, value);
	}

	/**
	 * Adds the provided {@link String} and {@link MinuteBitMap} into the map.
	 * Takes a deep copy of the statistic provided.
	 *
	 * @param string
	 * @param bitMap
	 */
	public void add(String string, MinuteBitMap bitMap) {
		if (!this.map.containsKey(string)) {
			this.map.put(string, bitMap.clone());
		} else {
			this.map.get(string).merge(bitMap);
		}
	}

	/**
	 * Adds the minute corresponding to the {@link Date} object given to the
	 * {@link MinuteBitMap} object corresponding to the key provided. If the key
	 * is not currently in the map, a new {@link MinuteBitMap} will be
	 * constructed.
	 * 
	 * @param string
	 * @param date
	 */
	public void add(String string, Date date) {
		if (!this.map.containsKey(string)) {
			this.map.put(string, new MinuteBitMap(date));
		} else {
			this.map.get(string).add(date);
		}
	}

	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MapOfMinuteBitMaps) {
			MapOfMinuteBitMaps otherMap = (MapOfMinuteBitMaps) s;
			for (String string : otherMap.getMap().keySet()) {
				if (!this.map.containsKey(string)) {
					this.map.put(string, otherMap.getMap().get(string).clone());
				} else {
					this.map.get(string).merge(otherMap.getMap().get(string));
				}
			}
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass() + " with a " + this.getClass());
		}
	}

	@Override
	public MapOfMinuteBitMaps clone() {
		return new MapOfMinuteBitMaps(this.map);
	}

	/**
	 * Returns the {@link MinuteBitMap} for the provided @link String}. It returns
	 * null if there is no value associated to the string.
	 * 
	 * @param s
	 * @return
	 */
	public MinuteBitMap getBitMap(String s) {
		return map.get(s);
	}

	/**
	 * Returns the map of {@link String}s to {@link MinuteBitMap}s.
	 * 
	 * @return
	 */
	public Map<String, MinuteBitMap> getMap() {
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
			MinuteBitMap bitmap = new MinuteBitMap();
			bitmap.readFields(in);
			this.map.put(s, bitmap);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.keySet().size());
		for (String s : map.keySet()) {
			Text.writeString(out, s);
			map.get(s).write(out);
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
		MapOfMinuteBitMaps other = (MapOfMinuteBitMaps) obj;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
