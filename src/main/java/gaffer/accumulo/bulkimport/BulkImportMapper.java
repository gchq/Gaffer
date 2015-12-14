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

import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for use in bulk import of data into Accumulo. It simply converts provided
 * {@link GraphElement}, {@link SetOfStatistics} pairs into Accumulo keys
 * and values using methods from {@link ConversionUtils}.
 */
public class BulkImportMapper extends Mapper<GraphElement, SetOfStatistics, Key, Value> {

	/**
	 * Converts the given {@link GraphElement} and {@link SetOfStatistics} into either one or
	 * two Accumulo {@link Key}, {@link Value} pairs, and outputs those.
	 *
	 * @param graphElement  The GraphElement to be converted into a Key
	 * @param setOfStatistics  The SetOfStatistics to be converted into a Value
	 * @param context  The Context to be used to output the (key, value) pairs
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(GraphElement graphElement, SetOfStatistics setOfStatistics, 
			Context context) throws IOException, InterruptedException {

		Pair<Key> pair = ConversionUtils.getKeysFromGraphElement(graphElement);
		Value value = ConversionUtils.getValueFromSetOfStatistics(setOfStatistics);
		context.write(pair.getFirst(), value);
		if (pair.getSecond() != null) {
			context.write(pair.getSecond(), value);
		}
		if (graphElement.isEntity()) {
			context.getCounter("Bulk import", "Entities").increment(1L);
		} else {
			context.getCounter("Bulk import", "Edges").increment(1L);
		}
	}

}
