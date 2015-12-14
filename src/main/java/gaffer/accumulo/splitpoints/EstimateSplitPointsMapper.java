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
package gaffer.accumulo.splitpoints;

import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class used for estimating the split points to ensure even distribution of
 * data in Accumulo after initial insert.
 */
public class EstimateSplitPointsMapper extends Mapper<GraphElement, SetOfStatistics, Key, Value> {

	private float proportion_to_sample;

	/**
	 * Retrieve the proportion to sample from the config. Defaults to 1 in a 
	 * thousand.
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		proportion_to_sample = context.getConfiguration().getFloat("proportion_to_sample", 0.001f);
	}

	/**
	 * Generates a random number and uses this to decide whether to ignore the
	 * GraphElement, SetOfStatistics pair or not. If it doesn't ignore it, it
	 * converts it to (one or two) Accumulo key-value pairs and outputs that.  
	 */
	protected void map(GraphElement graphElement, SetOfStatistics setOfStatistics, 
			Context context) throws IOException, InterruptedException {
		if (Math.random() < proportion_to_sample) {
			context.getCounter("Split points", "Number sampled").increment(1L);
			Pair<Key> pair = ConversionUtils.getKeysFromGraphElement(graphElement);
			Value value = ConversionUtils.getValueFromSetOfStatistics(setOfStatistics);
			context.write(pair.getFirst(), value);
			if (pair.getSecond() != null) {
				context.write(pair.getSecond(), value);
			}
		} else {
			context.getCounter("Split points", "Number not sampled").increment(1L);
		}
	}

}
