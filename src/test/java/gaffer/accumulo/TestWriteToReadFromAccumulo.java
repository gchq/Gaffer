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
package gaffer.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gaffer.Pair;
import gaffer.graph.Edge;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
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
 * Uses a mock Accumulo instance to check that we can write an edge to
 * Accumulo and read the same edge back out.
 */
public class TestWriteToReadFromAccumulo {

	@Test
	public void testWritingToAndReadingFromAccumulo() {
		Instance instance = new MockInstance();
		try {
			String tableName = "Test";
			String visibilityString = "public";

			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			conn.tableOperations().create(tableName);

			// Create start and end dates for edge
			Calendar startCalendar = new GregorianCalendar();
			startCalendar.clear();
			startCalendar.set(2014, Calendar.JANUARY, 1);
			Date startDate = startCalendar.getTime();
			Calendar endCalendar = new GregorianCalendar();
			endCalendar.clear();
			endCalendar.set(2014, Calendar.JANUARY, 2);
			Date endDate = endCalendar.getTime();

			// Create edge
			Edge originalEdge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString, startDate, endDate);

			// Create statistics
			SetOfStatistics originalStatistics = new SetOfStatistics();
			originalStatistics.addStatistic("count", new Count(1));

			// Accumulo key
			Pair<Key> pair = ConversionUtils.getKeysFromEdge(originalEdge);
			Key key1 = pair.getFirst();
			Key key2 = pair.getSecond();

			// Accumulo value
			Value value = ConversionUtils.getValueFromSetOfStatistics(originalStatistics);

			// Create mutation
			Mutation m1 = new Mutation(key1.getRow());
			m1.put(key1.getColumnFamily(), key1.getColumnQualifier(), new ColumnVisibility(key1.getColumnVisibility()), key1.getTimestamp(), value);
			Mutation m2 = new Mutation(key2.getRow());
			m2.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value);
			
			// Write mutation
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			writer.addMutation(m1);
			writer.addMutation(m2);
			writer.close();

			// Read back
			Authorizations authorizations = new Authorizations(visibilityString);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			// First key, value
			Entry<Key,Value> entry1 = it.next();
			Edge readEdge1 = ConversionUtils.getEdgeFromKey(entry1.getKey());
			assertEquals(readEdge1, originalEdge);
			SetOfStatistics readStatistics1 =  ConversionUtils.getSetOfStatisticsFromValue(entry1.getValue());
			assertEquals(readStatistics1, originalStatistics);
			// Second key, value
			Entry<Key,Value> entry2 = it.next();
			Edge readEdge2 = ConversionUtils.getEdgeFromKey(entry2.getKey());
			assertEquals(readEdge2, originalEdge);
			SetOfStatistics readStatistics2 =  ConversionUtils.getSetOfStatisticsFromValue(entry2.getValue());
			assertEquals(readStatistics2, originalStatistics);
			
			// Check no more entries
			if (it.hasNext()) {
				fail("Additional row found.");
			}

		} catch (AccumuloException e) {
			fail(this.getClass().getSimpleName() + " failed with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail(this.getClass().getSimpleName() + " failed with exception: " + e);
		} catch (TableExistsException e) {
			fail(this.getClass().getSimpleName() + " failed with exception: " + e);
		} catch (TableNotFoundException e) {
			fail(this.getClass().getSimpleName() + " failed with exception: " + e);
		} catch (IOException e) {
			fail(this.getClass().getSimpleName() + " failed with exception: " + e);
		}
	}

}
