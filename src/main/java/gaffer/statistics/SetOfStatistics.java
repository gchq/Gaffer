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
package gaffer.statistics;

import gaffer.graph.Edge;
import gaffer.graph.Entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A wrapper for a map from a {@link String} to a {@link Statistic}. This
 * allows multiple statistics to be stored for an {@link Entity} or {@link Edge}.
 * The string uniquely identifies the individual {@link Statistic}.
 * Multiple statistics of the same type can be stored within a
 * {@link SetOfStatistics}.
 * 
 * Two {@link SetOfStatistics}s can be merged together - this allows information
 * about an edge from different time periods to be merged together.
 */
public class SetOfStatistics implements Writable, Serializable {

	private static final long serialVersionUID = -2202570354159036495L;
	private Map<String, Statistic> nameToStat;

	public SetOfStatistics() {
		this.nameToStat = new HashMap<String, Statistic>();
	}

	public SetOfStatistics(String name, Statistic stat) {
		this();
		addStatistic(name, stat);
	}

	/**
	 * Adds a statistic with a given name.
	 * 
	 * @param name
	 * @param stat
	 */
	public void addStatistic(String name, Statistic stat) {
		this.nameToStat.put(name, stat);
	}
	
	/**
	 * Merges another {@link SetOfStatistics} into this one. The
	 * statistics from s are cloned if they are added in.
	 * 
	 * @param s
	 * @throws IllegalArgumentException
	 */
	public void merge(SetOfStatistics s) throws IllegalArgumentException {
		Map <String, Statistic> otherNameToStat = s.nameToStat;
		// Iterate through names in s to be merged in, and merge
		// them in.
		for (String name : otherNameToStat.keySet()) {
			if (this.containsStatWithThisName(name)) {
				Statistic otherStatistic = otherNameToStat.get(name);
				this.nameToStat.get(name).merge(otherStatistic);
			} else {
				this.addStatistic(name, otherNameToStat.get(name).clone());
			}
		}
	}
	
	/**
	 * Returns a deep copy of this object.
	 * 
	 * @return
	 */
	public SetOfStatistics clone() {
		SetOfStatistics clone = new SetOfStatistics();
		clone.merge(this);
		return clone;
	}
	
	private boolean containsStatWithThisName(String name) {
		return nameToStat.containsKey(name);
	}

	/**
	 * Returns the {@link Statistic} corresponding to the provided
	 * name. If there is no {@link Statistic} corresponding to
	 * the name then null is returned.
	 * 
	 * @param name
	 * @return
	 */
	public Statistic getStatisticByName(String name) {
		return nameToStat.get(name);
	}
	
	/**
	 * Returns a map from the statistic name to the statistic. This is not
	 * a copy of the underlying map, so changes to this map will effect this
	 * object. This avoids unnecessary cloning.
	 * 
	 * @return
	 */
	public Map<String, Statistic> getStatistics() {
		return nameToStat;
	}

	/**
	 * Removes all entries from this map.
	 */
	public void clear() {
		this.nameToStat.clear();
	}

	/**
	 * Removes the {@link Statistic} associated to the given <code>name</code>.
	 *
	 * @param name
	 */
	public void removeStatistic(String name) {
		nameToStat.remove(name);
	}

	/**
	 * Removes all {@link Statistic}s with names that are not in the provided set.
	 *
	 * @param statisticsToKeep
	 */
	public void keepOnlyTheseStatistics(Set<String> statisticsToKeep) {
		Iterator<Map.Entry<String, Statistic>> it = nameToStat.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Statistic> entry = it.next();
			if (!statisticsToKeep.contains(entry.getKey())) {
				it.remove();
			}
		}
	}

	/**
	 * Removes all {@link Statistic}s with names that are in the provided set.
	 *
	 * @param statisticsToRemove
	 */
	public void removeTheseStatistics(Set<String> statisticsToRemove) {
		Iterator<Map.Entry<String, Statistic>> it = nameToStat.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Statistic> entry = it.next();
			if (statisticsToRemove.contains(entry.getKey())) {
				it.remove();
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.nameToStat.clear();
		int numberOfItems = in.readInt();
		for (int i = 0; i < numberOfItems; i++) {
			String key = Text.readString(in);
			nameToStat.put(key, StatisticsUtilities.getStatistic(in));
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(nameToStat.keySet().size());
		for (String key : nameToStat.keySet()) {
			Text.writeString(out, key);
			// If the statistic is one of the internal Gaffer ones, then output the
			// simple name. Otherwise output the full class name.
			Statistic stat = nameToStat.get(key);
			if (StatisticsUtilities.isStatisticInternal(stat)) {
				Text.writeString(out, stat.getClass().getSimpleName());
			} else {
				Text.writeString(out, stat.getClass().getName());
			}
			stat.write(out);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String key : nameToStat.keySet()) {
			if (i > 0) {
				sb.append(", ");
			}
			sb.append(key);
			sb.append("=");
			sb.append(nameToStat.get(key));
			i++;
		}
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((nameToStat == null) ? 0 : nameToStat.hashCode());
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
		SetOfStatistics other = (SetOfStatistics) obj;
		if (nameToStat == null) {
			if (other.nameToStat != null)
				return false;
		} else if (!nameToStat.equals(other.nameToStat))
			return false;
		return true;
	}

}
