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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.TableUtils;
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
 * Test that {@link SetOfStatisticsCombiner} correctly combines multiple
 * {@link SetOfStatistics} when used.
 */
public class TestSetOfStatisticsCombiner {

	@Test
	public void test() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		String visibilityString = "public";

		long currentTime = System.currentTimeMillis();
		Calendar currentCalendar = new GregorianCalendar();
		currentCalendar.setTime(new Date(currentTime));

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
			TableUtils.createTable(conn, tableName, (30 * 24 * 60 * 60 * 1000L)); // NB: Essential to have the 'L' here so that
			// it is a long (if not then multiplication
			// happens as int and overflows).

			// Create start and end dates for edge (from a week before the current date)
			Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
			sevenDaysBeforeCalendar.setTime(new Date(currentTime));
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
			Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();

			// Create edge
			Edge edge = new Edge("customer", "A", "product", "X", "purchase", "instore", true, visibilityString, sevenDaysBefore, sixDaysBefore);

			// Create some SetOfStatistics
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(10));

			// Create merged SetOfStatistics - we expect to get this back from the table
			SetOfStatistics mergedStatistics = new SetOfStatistics();
			mergedStatistics.merge(statistics1);
			mergedStatistics.merge(statistics2);
			mergedStatistics.merge(statistics3);

			// Accumulo key
			Key key = ConversionUtils.getKeysFromEdge(edge).getFirst();

			// Accumulo values
			Value value1 = ConversionUtils.getValueFromSetOfStatistics(statistics1);
			Value value2 = ConversionUtils.getValueFromSetOfStatistics(statistics2);
			Value value3 = ConversionUtils.getValueFromSetOfStatistics(statistics3);

			// Create mutation
			Mutation m1 = new Mutation(key.getRow());
			m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
			Mutation m2 = new Mutation(key.getRow());
			m2.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value2);
			Mutation m3 = new Mutation(key.getRow());
			m3.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value3);

			// Write mutation
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			writer.addMutation(m1);
			writer.addMutation(m2);
			writer.addMutation(m3);
			writer.close();

			// Read data back and check we get one merged SetOfStatistics
			Authorizations authorizations = new Authorizations(visibilityString);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			Entry<Key,Value> entry = it.next();
			Edge readEdge = ConversionUtils.getEdgeFromKey(entry.getKey());
			assertEquals(readEdge, edge);
			SetOfStatistics readStatistics =  ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(readStatistics, mergedStatistics);

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

