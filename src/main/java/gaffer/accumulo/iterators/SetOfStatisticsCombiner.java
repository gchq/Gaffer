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
package gaffer.accumulo.iterators;

import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.Constants;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

/**
 * Combines {@link SetOfStatistics} for the same key. This is done by initialising
 * a new {@link SetOfStatistics}, iterating through each {@link Value}, converting each to
 * a {@link SetOfStatistics} and merging it into the initial {@link SetOfStatistics}. This
 * is then converted into a {@link Value} which is output.
 * 
 * If there is only one {@link Value} then we simply return it straight away. This
 * avoids the expense of deserialising the Value into a {@link SetOfStatistics} and
 * reserialising.
 * 
 * If there is an {@link IOException} converting the {@link Value} into a
 * {@link SetOfStatistics} then we log it by adding a statistic to the resulting values.
 */
public class SetOfStatisticsCombiner extends Combiner {

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {
		// Get first Value. If this is the only Value then return it straight
		// away.
		Value value = iter.next();
		if (!iter.hasNext()) {
			return value;
		}
		// There is more than one Value. Take the Value we have already read,
		// convert it to a SetOfStatistics and merge it into a newly created
		// SetOfStatistics. Then iterate through the rest of the Values and
		// merge those in. Finally convert the merged SetOfStatistics into a
		// Value and return.
		SetOfStatistics statistics = new SetOfStatistics();
		int numberOfExceptions = 0;
		try {
			statistics.merge(ConversionUtils.getSetOfStatisticsFromValue(value));
		} catch (IOException e) {
			numberOfExceptions++;
		}
		while (iter.hasNext()) {
			try {
				statistics.merge(ConversionUtils.getSetOfStatisticsFromValue(iter.next()));
			} catch (IOException e) {
				numberOfExceptions++;
			}
		}
		if (numberOfExceptions != 0) {
			statistics.addStatistic(Constants.ERROR_IN_SET_OF_STATISTICS_COMBINER_ITERATOR, new Count(numberOfExceptions));
		}
		return ConversionUtils.getValueFromSetOfStatistics(statistics);
	}

}
