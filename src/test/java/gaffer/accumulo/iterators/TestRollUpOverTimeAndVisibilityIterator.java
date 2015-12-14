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
import gaffer.GraphAccessException;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.TableUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
 * Unit tests for the {@link RollUpOverTimeAndVisibility} iterator.
 */
public class TestRollUpOverTimeAndVisibilityIterator {

	@Test
	public void testEdgeRollUp() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds
		// NB: Essential to have the 'L' here so that
		// it is a long (if not then multiplication
		// happens as int and overflows).

		long currentTime = System.currentTimeMillis();
		Calendar currentCalendar = new GregorianCalendar();
		currentCalendar.setTime(new Date(currentTime));

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Dates for edges - two separate 24 hour periods from around week before the current date
			Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
			sevenDaysBeforeCalendar.setTime(new Date(currentTime));
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
			Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date fiveDaysBefore = sevenDaysBeforeCalendar.getTime();

			// Create some visibilities
			String visibilityString1 = "private";
			String visibilityString2 = "public";

			// Edge 1 and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "C", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));

			// Edge 2 and some statistics for it
			Edge edge2 = new Edge("customer", "A", "product", "C", "purchase", "instore", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));

			// Edge 3 and some statistics for it
			Edge edge3 = new Edge("customer", "A", "product", "C", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));

			// Edge 4 and some statistics for it
			Edge edge4 = new Edge("customer", "A", "product", "C", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));

			// Edge 5 and some statistics for it
			Edge edge5 = new Edge("customer", "B", "product", "D", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			statistics5.addStatistic("count", new Count(99));

			// Expected results
			//    - from edge 4			
			Edge expectedResult1 = new Edge("customer", "A", "product", "C", "purchase", "instore", false,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
			expectedSetOfStatistics1.addStatistic("countSomething", new Count(123456));
			//    - from the combination of edges 1, 2 and 3
			Edge expectedResult2 = new Edge("customer", "A", "product", "C", "purchase", "instore", true,
					visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
			SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
			expectedSetOfStatistics2.addStatistic("count", new Count(20));
			expectedSetOfStatistics2.addStatistic("anotherCount", new Count(1000000));
			//    - from edge 5		
			Edge expectedResult3 = new Edge("customer", "B", "product", "D", "purchase", "instore", true,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
			expectedSetOfStatistics3.addStatistic("count", new Count(99));

			// Accumulo keys
			Key key1 = ConversionUtils.getKeysFromEdge(edge1).getFirst();
			Key key2 = ConversionUtils.getKeysFromEdge(edge2).getFirst();
			Key key3 = ConversionUtils.getKeysFromEdge(edge3).getFirst();
			Key key4 = ConversionUtils.getKeysFromEdge(edge4).getFirst();
			Key key5 = ConversionUtils.getKeysFromEdge(edge5).getFirst();

			// Accumulo values
			Value value1 = ConversionUtils.getValueFromSetOfStatistics(statistics1);
			Value value2 = ConversionUtils.getValueFromSetOfStatistics(statistics2);
			Value value3 = ConversionUtils.getValueFromSetOfStatistics(statistics3);
			Value value4 = ConversionUtils.getValueFromSetOfStatistics(statistics4);
			Value value5 = ConversionUtils.getValueFromSetOfStatistics(statistics5);

			// Create mutations
			Mutation m1 = new Mutation(key1.getRow());
			m1.put(key1.getColumnFamily(), key1.getColumnQualifier(), new ColumnVisibility(key1.getColumnVisibility()), key1.getTimestamp(), value1);
			Mutation m2 = new Mutation(key2.getRow());
			m2.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value2);
			Mutation m3 = new Mutation(key3.getRow());
			m3.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value3);
			Mutation m4 = new Mutation(key4.getRow());
			m4.put(key4.getColumnFamily(), key4.getColumnQualifier(), new ColumnVisibility(key4.getColumnVisibility()), key4.getTimestamp(), value4);
			Mutation m5 = new Mutation(key5.getRow());
			m5.put(key5.getColumnFamily(), key5.getColumnQualifier(), new ColumnVisibility(key5.getColumnVisibility()), key5.getTimestamp(), value5);

			// Write mutations
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			writer.addMutation(m1);
			writer.addMutation(m2);
			writer.addMutation(m3);
			writer.addMutation(m4);
			writer.addMutation(m5);
			writer.close();

			// Set up scanner - remember to add roll up iterator
			Authorizations authorizations = new Authorizations(visibilityString1, visibilityString2);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			TableUtils.addRollUpOverTimeAndVisibilityIteratorToScanner(scanner);

			// Read data back and check we get the correct answers
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			Entry<Key,Value> entry = it.next();
			Edge readEdge1 = ConversionUtils.getEdgeFromKey(entry.getKey());
			SetOfStatistics readSetOfStatistics1 = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(expectedResult1, readEdge1);
			assertEquals(expectedSetOfStatistics1, readSetOfStatistics1);
			entry = it.next();
			Edge readEdge2 = ConversionUtils.getEdgeFromKey(entry.getKey());
			SetOfStatistics readSetOfStatistics2 = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(expectedResult2, readEdge2);
			assertEquals(expectedSetOfStatistics2, readSetOfStatistics2);
			entry = it.next();
			Edge readEdge3 = ConversionUtils.getEdgeFromKey(entry.getKey());
			SetOfStatistics readSetOfStatistics3 = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(expectedResult3, readEdge3);
			assertEquals(expectedSetOfStatistics3, readSetOfStatistics3);

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

	@Test
	public void testEntityRollUp() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds
		// NB: Essential to have the 'L' here so that
		// it is a long (if not then multiplication
		// happens as int and overflows).

		long currentTime = System.currentTimeMillis();
		Calendar currentCalendar = new GregorianCalendar();
		currentCalendar.setTime(new Date(currentTime));

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Dates for entities - two separate 24 hour periods from around week before the current date
			Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
			sevenDaysBeforeCalendar.setTime(new Date(currentTime));
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
			Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date fiveDaysBefore = sevenDaysBeforeCalendar.getTime();

			// Create some visibilities
			String visibilityString1 = "private";
			String visibilityString2 = "public";

			// Entity 1 and some statistics for it
			Entity entity1 = new Entity("customer", "A", "info", "subinfo", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));

			// Entity 2 and some statistics for it
			Entity entity2 = new Entity("customer", "A", "info", "subinfo", visibilityString1, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));

			// Entity 3 and some statistics for it
			Entity entity3 = new Entity("customer", "A", "info", "subinfo", visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));

			// Entity 4 and some statistics for it
			Entity entity4 = new Entity("customer", "A", "moreinfo", "subinfo", visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));

			// Entity 5 and some statistics for it
			Entity entity5 = new Entity("customer", "B", "info", "subinfo", visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			statistics5.addStatistic("count", new Count(99));

			// Create GraphElementWithStatistics ready for adding to Accumulo
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();
			data.add(new GraphElementWithStatistics(new GraphElement(entity1), statistics1));
			data.add(new GraphElementWithStatistics(new GraphElement(entity2), statistics2));
			data.add(new GraphElementWithStatistics(new GraphElement(entity3), statistics3));
			data.add(new GraphElementWithStatistics(new GraphElement(entity4), statistics4));
			data.add(new GraphElementWithStatistics(new GraphElement(entity5), statistics5));

			// Add data to Accumulo
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);
			graph.addGraphElementsWithStatistics(data);

			// Expected results
			//    - from the combination of entities 1, 2 and 3
			Entity expectedResult1 = new Entity("customer", "A", "info", "subinfo",
					visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
			SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
			expectedSetOfStatistics1.addStatistic("count", new Count(20));
			expectedSetOfStatistics1.addStatistic("anotherCount", new Count(1000000));

			//    - from entity 4
			Entity expectedResult2 = new Entity("customer", "A", "moreinfo", "subinfo",
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
			expectedSetOfStatistics2.addStatistic("countSomething", new Count(123456));

			//    - from entity 5		
			Entity expectedResult3 = new Entity("customer", "B", "info", "subinfo",
					visibilityString2, sixDaysBefore, fiveDaysBefore);

			SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
			expectedSetOfStatistics3.addStatistic("count", new Count(99));

			// Set up scanner - remember to add roll up iterator
			Authorizations authorizations = new Authorizations(visibilityString1, visibilityString2);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			TableUtils.addRollUpOverTimeAndVisibilityIteratorToScanner(scanner);

			// Read data back and check we get the correct answers
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			Entry<Key,Value> entry = it.next();
			Entity readEntity1 = ConversionUtils.getEntityFromKey(entry.getKey());
			SetOfStatistics readSetOfStatistics1 = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(expectedResult1, readEntity1);
			assertEquals(expectedSetOfStatistics1, readSetOfStatistics1);
			entry = it.next();
			Entity readEntity2 = ConversionUtils.getEntityFromKey(entry.getKey());
			SetOfStatistics readSetOfStatistics2 = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(expectedResult2, readEntity2);
			assertEquals(expectedSetOfStatistics2, readSetOfStatistics2);
			entry = it.next();
			Entity readEntity3 = ConversionUtils.getEntityFromKey(entry.getKey());
			SetOfStatistics readSetOfStatistics3 = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(expectedResult3, readEntity3);
			assertEquals(expectedSetOfStatistics3, readSetOfStatistics3);

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
		} catch (GraphAccessException e) {
			fail(this.getClass().getSimpleName() + " failed with exception: " + e);
		}

	}

}
