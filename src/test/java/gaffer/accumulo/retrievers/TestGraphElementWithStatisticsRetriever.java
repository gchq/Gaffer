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
package gaffer.accumulo.retrievers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import gaffer.CloseableIterable;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.TableUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.accumulo.predicate.RawGraphElementWithStatisticsPredicate;
import gaffer.accumulo.retrievers.impl.GraphElementWithStatisticsRetrieverFromEntities;
import gaffer.graph.Edge;
import gaffer.graph.TypeValue;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.Predicate;
import gaffer.predicate.time.impl.TimeWindowPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

/**
 * Unit test for {@link GraphElementWithStatisticsRetrieverFromEntities}. In particular tests that if more entities
 * are queried for than will fit into one {@link BatchScanner} then the correct number of
 * entries are returned. Most functionality of this class is tested via the unit tests for
 * {@link AccumuloBackedGraph}.
 */
public class TestGraphElementWithStatisticsRetriever {

	private static Date sevenDaysBefore;
	private static Date sixDaysBefore;
	private static String visibilityString = "public";

	static {
		Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
		sevenDaysBeforeCalendar.setTime(new Date(System.currentTimeMillis()));
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
		sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
		sixDaysBefore = sevenDaysBeforeCalendar.getTime();
	}

	@Test
	public void test() {
		try {
			// Create mock Accumulo instance and table
			Instance instance = new MockInstance();
			String tableName = "Test";
			Connector conn = instance.getConnector("user", "password");

			// Populate graph
			int numEntries = 1000;
			setupGraph(instance, conn, tableName, numEntries);
			// Create set to query for
			Set<TypeValue> typeValues = new HashSet<TypeValue>();
			for (int i = 0; i < 1000; i++) {
				typeValues.add(new TypeValue("customer", "" + i));
			}

			// Retrieve elements when less TypeValues are provided than the max number of entries for the batch scanner
			Predicate<RawGraphElementWithStatistics> predicate = new RawGraphElementWithStatisticsPredicate(
					new TimeWindowPredicate(sevenDaysBefore, sixDaysBefore));
			CloseableIterable<GraphElementWithStatistics> retriever = new GraphElementWithStatisticsRetrieverFromEntities(conn, new Authorizations(visibilityString), tableName, 10000, 2,
					true, predicate, null, null, true, true, typeValues);
			int count = 0;
			for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStatistics : retriever) {
				count++;
			}
			assertEquals(numEntries, count);

			// Retrieve elements when more TypeValues are provided than the max number of entries for the batch scanner
			retriever = new GraphElementWithStatisticsRetrieverFromEntities(conn, new Authorizations(visibilityString), tableName, 100, 2,
					true, predicate, null, null, true, true, typeValues);
			count = 0;
			for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStatistics : retriever) {
				count++;
			}
			assertEquals(numEntries, count);

			// Retrieve elements when the same number of TypeValues are provided as the max number of entries for the batch scanner
			retriever = new GraphElementWithStatisticsRetrieverFromEntities(conn, new Authorizations(visibilityString), tableName, 1000, 2,
					true, predicate, null, null, true, true, typeValues);
			count = 0;
			for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStatistics : retriever) {
				count++;
			}
			assertEquals(numEntries, count);
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
	}

	private static AccumuloBackedGraph setupGraph(Instance instance, Connector conn, String tableName, int numEntries) {
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

		try {
			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Create numEntries edges and add to Accumulo
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			for (int i = 0; i < numEntries; i++) {
				Edge edge = new Edge("customer", "" + i, "product", "B", "purchase", "instore", true, visibilityString, sevenDaysBefore, sixDaysBefore);
				SetOfStatistics statistics = new SetOfStatistics();
				statistics.addStatistic("count", new Count(i));
				Key key = ConversionUtils.getKeysFromEdge(edge).getFirst();
				Value value = ConversionUtils.getValueFromSetOfStatistics(statistics);
				Mutation m = new Mutation(key.getRow());
				m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
				writer.addMutation(m);
			}
			writer.close();

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);
			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

}
