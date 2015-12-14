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
package gaffer.accumulo.insert;

import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Utility functions to enable data to be added directly to a graph (rather than
 * via the bulk import mechanism). 
 */
public class InsertIntoGraphUtilities {

	private InsertIntoGraphUtilities() { }

	/**
	 * Inserts data from the provided {@link Iterable} of {@link GraphElement}s
	 * into the Accumulo table.
	 * 
	 * @param connector  A Connector to Accumulo
	 * @param tableName  The name of the table
	 * @param bufferSize  The size in bytes of the maximum memory to use for batching data before writing
	 * @param timeOut  The time in milliseconds to wait before writing a batch
	 * @param numThreads  The number of threads to use for writing data to the tablet servers
	 * @param graphElements  The Iterable of GraphElements to be written to the table
	 * @throws TableNotFoundException
	 * @throws MutationsRejectedException
	 */
	public static void insertGraphElements(Connector connector, String tableName, long bufferSize, long timeOut, int numThreads,
			Iterable<GraphElement> graphElements) throws TableNotFoundException, MutationsRejectedException {
		// As no SetOfStatistics are provided in this method, just create empty one, get Value from that
		// and use that for every mutation.
		Value value = ConversionUtils.getValueFromSetOfStatistics(new SetOfStatistics());
		// Create BatchWriter
		BatchWriter writer = connector.createBatchWriter(tableName, bufferSize, timeOut, numThreads);
		// Loop through elements, convert to mutations, and add to BatchWriter.
		// The BatchWriter takes care of batching them up, sending them without too high a latency, etc.
		for (GraphElement graphElement : graphElements) {
			Pair<Key> keys = ConversionUtils.getKeysFromGraphElement(graphElement);
			Mutation m = new Mutation(keys.getFirst().getRow());
			m.put(keys.getFirst().getColumnFamily(), keys.getFirst().getColumnQualifier(),
					new ColumnVisibility(keys.getFirst().getColumnVisibility()), keys.getFirst().getTimestamp(), value);
			writer.addMutation(m);
			// If the GraphElement is an Entity then there will only be 1 key, and the second will be null.
			// If the GraphElement is an Edge then there will be 2 keys.
			if (keys.getSecond() != null) {
				Mutation m2 = new Mutation(keys.getSecond().getRow());
				m2.put(keys.getSecond().getColumnFamily(), keys.getSecond().getColumnQualifier(),
						new ColumnVisibility(keys.getSecond().getColumnVisibility()), keys.getSecond().getTimestamp(), value);
				writer.addMutation(m2);
			}
		}
		writer.close();
	}

	/**
	 * Inserts data from the provided {@link Iterable} of {@link GraphElementWithStatistics}
	 * into the Accumulo table.
	 * 
	 * @param connector  A Connector to Accumulo
	 * @param tableName  The name of the table
	 * @param bufferSize  The size in bytes of the maximum memory to use for batching data before writing
	 * @param timeOut  The time in milliseconds to wait before writing a batch
	 * @param numThreads  The number of threads to use for writing data to the tablet servers
	 * @param graphElementsWithStatistics  The Iterable of GraphElementWithStatistics to be written to the table
	 * @throws TableNotFoundException
	 * @throws MutationsRejectedException
	 */
	public static void insertGraphElementsWithStatistics(Connector connector, String tableName, long bufferSize, long timeOut, int numThreads,
			Iterable<GraphElementWithStatistics> graphElementsWithStatistics) throws TableNotFoundException, MutationsRejectedException {
		// Create BatchWriter
		BatchWriter writer = connector.createBatchWriter(tableName, bufferSize, timeOut, numThreads);
		// Loop through elements, convert to mutations, and add to BatchWriter.
		// The BatchWriter takes care of batching them up, sending them without too high a latency, etc.
		for (GraphElementWithStatistics graphElementWithStatistics : graphElementsWithStatistics) {
			Pair<Key> keys = ConversionUtils.getKeysFromGraphElement(graphElementWithStatistics.getGraphElement());
			Value value = ConversionUtils.getValueFromSetOfStatistics(graphElementWithStatistics.getSetOfStatistics());
			Mutation m = new Mutation(keys.getFirst().getRow());
			m.put(keys.getFirst().getColumnFamily(), keys.getFirst().getColumnQualifier(),
					new ColumnVisibility(keys.getFirst().getColumnVisibility()), keys.getFirst().getTimestamp(), value);
			writer.addMutation(m);
			// If the GraphElement is an Entity then there will only be 1 key, and the second will be null.
			// If the GraphElement is an Edge then there will be 2 keys.
			if (keys.getSecond() != null) {
				Mutation m2 = new Mutation(keys.getSecond().getRow());
				m2.put(keys.getSecond().getColumnFamily(), keys.getSecond().getColumnQualifier(),
						new ColumnVisibility(keys.getSecond().getColumnVisibility()), keys.getSecond().getTimestamp(), value);
				writer.addMutation(m2);
			}
		}
		writer.close();
	}

}
