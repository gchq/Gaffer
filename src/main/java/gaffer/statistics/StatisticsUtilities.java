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

import gaffer.statistics.impl.*;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Text;

/**
 * Utility methods for {@link Statistic}s.
 */
public class StatisticsUtilities {

	// INTERNAL_STATS are Statistics from Gaffer, i.e. in the statistics.impl package.
	// We maintain a map of them so that given the simple class name, e.g. Count, we can
	// quickly obtain an instance of that class (we then clone it and deserialise the statistic
	// into it).
	// EXTERNAL_STATS are user-provided ones - these are instantiated using reflection the first
	// time we encounter them, and on subsequent occasions, obtained from the EXTERNAL_STATS map
	// and then cloned.
	private static final Map<String, Statistic> INTERNAL_STATS = new HashMap<String, Statistic>();
	private static final Set<String> INTERNAL_STATS_FULL_NAMES = new HashSet<String>();
	private static final Map<String, Statistic> EXTERNAL_STATS = new ConcurrentHashMap<String, Statistic>();
	static {
		INTERNAL_STATS.put("CappedMinuteCount", new CappedMinuteCount());
		INTERNAL_STATS.put("CappedSetOfStrings", new CappedSetOfStrings());
		INTERNAL_STATS.put("Count", new Count());
		INTERNAL_STATS.put("DailyCount", new DailyCount());
		INTERNAL_STATS.put("DoubleCount", new DoubleCount());
		INTERNAL_STATS.put("DoubleProduct", new DoubleProduct());
		INTERNAL_STATS.put("DoubleProductViaLogs", new DoubleProductViaLogs());
		INTERNAL_STATS.put("FirstSeen", new FirstSeen());
		INTERNAL_STATS.put("HourlyBitMap", new HourlyBitMap());
		INTERNAL_STATS.put("HourlyCount", new HourlyCount());
		INTERNAL_STATS.put("HyperLogLogPlusPlus", new HyperLogLogPlusPlus());
		INTERNAL_STATS.put("IntArray", new IntArray());
		INTERNAL_STATS.put("IntMax", new IntMax());
		INTERNAL_STATS.put("IntMin", new IntMin());
		INTERNAL_STATS.put("IntSet", new IntSet());
		INTERNAL_STATS.put("LastSeen", new LastSeen());
		INTERNAL_STATS.put("LongCount", new LongCount());
		INTERNAL_STATS.put("LongMax", new LongMax());
		INTERNAL_STATS.put("LongMin", new LongMin());
		INTERNAL_STATS.put("MapFromDoubleToCount", new MapFromDoubleToCount());
		INTERNAL_STATS.put("MapFromIntToCount", new MapFromIntToCount());
		INTERNAL_STATS.put("MapFromIntToLongCount", new MapFromIntToLongCount());
		INTERNAL_STATS.put("MapFromStringToSetOfStrings", new MapFromStringToSetOfStrings());
		INTERNAL_STATS.put("MapOfCounts", new MapOfCounts());
		INTERNAL_STATS.put("MapOfLongCounts", new MapOfLongCounts());
		INTERNAL_STATS.put("MapOfMinuteBitMaps", new MapOfMinuteBitMaps());
		INTERNAL_STATS.put("MaxDouble", new MaxDouble());
		INTERNAL_STATS.put("MinDouble", new MinDouble());
		INTERNAL_STATS.put("MinuteBitMap", new MinuteBitMap());
		INTERNAL_STATS.put("SetOfStrings", new SetOfStrings());
		INTERNAL_STATS.put("ShortMax", new ShortMax());
		INTERNAL_STATS.put("ShortMin", new ShortMin());
		for (String statName : INTERNAL_STATS.keySet()) {
			INTERNAL_STATS_FULL_NAMES.add(INTERNAL_STATS.get(statName).getClass().getName());
		}
	}

	private StatisticsUtilities() {}

	/**
	 * Reads a {@link Statistic} from a stream of bytes. If the statistic is one of
	 * Gaffer's internal ones, then we get an empty version of it from the
	 * <code>INTERNAL_STATS</code> map, clone that and then deserialise it into that.
	 * If the statistic is not one of the internal ones, then we use reflection to
	 * create it the first time we see it, and store the empty version of it in the
	 * <code>EXTERNAL_STATS</code> map so that in future it can be created without
	 * using reflection.
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public static Statistic getStatistic(DataInput in) throws IOException {
		String statisticType = Text.readString(in);
		Statistic emptyStatistic = INTERNAL_STATS.get(statisticType);
		if (emptyStatistic != null) {
			// Need to clone the statistic so that we're not using the one in the map
			Statistic clonedStatistic = emptyStatistic.clone();
			clonedStatistic.readFields(in);
			return clonedStatistic;
		}
		// See if already in the EXTERNAL_STATS map
		emptyStatistic = EXTERNAL_STATS.get(statisticType);
		// If not already in the EXTERNAL_STATS map then create it by reflection, and add the empty version of it
		if (emptyStatistic == null) try {
			Class statisticClass = Class.forName(statisticType);
			Object obj = statisticClass.newInstance();
			if (!(obj instanceof Statistic)) {
				throw new IOException("Cannot instantiate Statistic of type " + statisticType);
			}
			EXTERNAL_STATS.put(statisticType, (Statistic) obj);
			emptyStatistic = (Statistic) obj;
		} catch (ClassNotFoundException e) {
			throw new IOException("Unknown statistic type " + statisticType + ": " + e);
		} catch (InstantiationException e) {
			throw new IOException("Unknown statistic type " + statisticType + ": " + e);
		} catch (IllegalAccessException e) {
			throw new IOException("Unknown statistic type " + statisticType + ": " + e);
		}
		Statistic clonedStatistic = emptyStatistic.clone();
		clonedStatistic.readFields(in);
		return clonedStatistic;
	}

	public static boolean isStatisticInternal(Statistic statistic) {
		return INTERNAL_STATS_FULL_NAMES.contains(statistic.getClass().getName());
	}

}
