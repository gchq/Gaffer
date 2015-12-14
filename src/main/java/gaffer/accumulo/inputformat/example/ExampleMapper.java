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
package gaffer.accumulo.inputformat.example;

import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * An example of a {@link Mapper} that consumes data from the Accumulo underlying
 * Gaffer. This is just the identity mapper but it does include counters to indicate
 * the number of {@link Entity}s and {@link Edge}s in the graph. 
 */
public class ExampleMapper extends Mapper<GraphElement, SetOfStatistics, GraphElement, SetOfStatistics> {

	protected void map(GraphElement element, SetOfStatistics setOfStatistics, 
			Context context) throws IOException, InterruptedException {
		if (element.isEntity()) {
			context.getCounter("Gaffer input format", "Entities").increment(1L);
		} else {
			context.getCounter("Gaffer input format", "Edges").increment(1L);
		}
		context.write(element, setOfStatistics);
	}

}
