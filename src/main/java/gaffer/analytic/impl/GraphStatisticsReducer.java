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
package gaffer.analytic.impl;

import gaffer.statistics.SetOfStatistics;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Simply merges together all the {@link SetOfStatistics} for the key.
 */
public class GraphStatisticsReducer extends Reducer<Text, SetOfStatistics, Text, SetOfStatistics> {

	protected void reduce(Text key, Iterable<SetOfStatistics> values, Context context) throws IOException, InterruptedException {
		Iterator<SetOfStatistics> iter = values.iterator();
		SetOfStatistics statistics = new SetOfStatistics();
		while (iter.hasNext()) {
			statistics.merge(iter.next());
		}
		context.write(key, statistics);
	}
	
}

