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
package gaffer.accumulo.bulkimport;

import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.Constants;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for use in bulk import of data into Accumulo. It merges all values associated
 * to the key by converting them into {@link SetOfStatistics} and then merges
 * those, and then converts them back to an Accumulo value.
 * 
 * It contains an optimisation so that if there is only one value, we simply
 * output it rather than incurring the cost of deserialising them and then
 * reserialising them.
 */
public class BulkImportReducer extends Reducer<Key, Value, Key, Value> {

	/**
	 * Deserialises all the given {@link Value}s into {@link SetOfStatistics}, merges
	 * those together, serialises those to a {@link Value} and then outputs
	 * that with key as the key. Also increments counters to indicate how
	 * many statistics have been merged.
	 *
	 * @param key  The key
	 * @param values  The Values associated with the Key
	 * @param context  The Context to be used to output the (key, value) pairs
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(Key key, Iterable<Value> values, Context context)
			throws IOException, InterruptedException {

		// Get first Value. If this is the only Value then return it straight
		// away.
		Iterator<Value> iter = values.iterator();
		Value value = iter.next();
		if (!iter.hasNext()) {
			context.write(key, value);
			context.getCounter("Bulk import", "Only 1 value").increment(1L);
			return;
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
			context.getCounter("Bulk import", "Exception converting value to stats").increment(1L);
		}
		context.write(key, ConversionUtils.getValueFromSetOfStatistics(statistics));
		context.getCounter("Bulk import", ">1 value").increment(1L);
	}

}
