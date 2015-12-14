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

/**
 * Unit tests for {@link TableUtils}.
 */
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gaffer.Pair;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
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
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

public class TestTableUtils {

	@Test
	public void testAgeOffOfEntities() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		String visibilityString = "public";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

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

			// FIRST ENTITY - from a week before the current date
			Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
			sevenDaysBeforeCalendar.setTime(new Date(currentTime));
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
			Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();

			// Create entity
			Entity entity1 = new Entity("customer", "customer", "purchase", "product", visibilityString, sevenDaysBefore, sixDaysBefore);

			// Create some SetOfStatistics
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));

			// SECOND ENTITY - from 10 weeks before the current date
			Calendar tenWeeksBeforeCalendar = new GregorianCalendar();
			tenWeeksBeforeCalendar.setTime(new Date(currentTime));
			tenWeeksBeforeCalendar.add(Calendar.WEEK_OF_YEAR, -10);
			Date tenWeeksBefore = tenWeeksBeforeCalendar.getTime();
			tenWeeksBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date tenWeeksBeforePlusADay = tenWeeksBeforeCalendar.getTime();

			// Create entity
			Entity entity2 = new Entity("customer", "customer", "purchase", "product", visibilityString, tenWeeksBefore, tenWeeksBeforePlusADay);

			// Create some SetOfStatistics
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(5));

			// Accumulo keys
			Key key1 = ConversionUtils.getKeyFromEntity(entity1);
			Key key2 = ConversionUtils.getKeyFromEntity(entity2);

			// Accumulo values
			Value value1 = ConversionUtils.getValueFromSetOfStatistics(statistics1);
			Value value2 = ConversionUtils.getValueFromSetOfStatistics(statistics2);

			// Create mutation
			Mutation m1 = new Mutation(key1.getRow());
			m1.put(key1.getColumnFamily(), key1.getColumnQualifier(), new ColumnVisibility(key1.getColumnVisibility()), key1.getTimestamp(), value1);
			Mutation m2 = new Mutation(key2.getRow());
			m2.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value2);

			// Write mutation
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			writer.addMutation(m1);
			writer.addMutation(m2);
			writer.close();

			// Read data back and check we get two identical edges
			Authorizations authorizations = new Authorizations(visibilityString);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			Entry<Key,Value> entry = it.next();
			Entity readEntity = ConversionUtils.getEntityFromKey(entry.getKey());
			SetOfStatistics readStatistics =  ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			
			// Check no more entries
			if (it.hasNext()) {
				fail("Additional row found.");
			}

			// Check entries read are correct (i.e. come from entity 1)
			assertEquals(entity1, readEntity);
			assertEquals(statistics1, readStatistics);

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
	public void testAgeOffOfEdges() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		String visibilityString = "public";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

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

			// FIRST EDGE - from a week before the current date
			Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
			sevenDaysBeforeCalendar.setTime(new Date(currentTime));
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
			Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();

			// Create edge
			Edge edge1 = new Edge("customer", "customer", "product", "P", "communication", "customer", true, visibilityString, sevenDaysBefore, sixDaysBefore);

			// Create some SetOfStatistics
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));

			// SECOND EDGE - from 10 weeks before the current date
			Calendar tenWeeksBeforeCalendar = new GregorianCalendar();
			tenWeeksBeforeCalendar.setTime(new Date(currentTime));
			tenWeeksBeforeCalendar.add(Calendar.WEEK_OF_YEAR, -10);
			Date tenWeeksBefore = tenWeeksBeforeCalendar.getTime();
			tenWeeksBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date tenWeeksBeforePlusADay = tenWeeksBeforeCalendar.getTime();

			// Create edge
			Edge edge2 = new Edge("customer", "customer", "product", "P", "communication", "customer", true, visibilityString, tenWeeksBefore, tenWeeksBeforePlusADay);

			// Create some SetOfStatistics
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(5));

			// Accumulo keys
			Pair<Key> pair1 = ConversionUtils.getKeysFromEdge(edge1);
			Key key1A = pair1.getFirst();
			Key key1B = pair1.getSecond();
			Pair<Key> pair2 = ConversionUtils.getKeysFromEdge(edge2);
			Key key2A = pair2.getFirst();
			Key key2B = pair2.getSecond();

			// Accumulo values
			Value value1 = ConversionUtils.getValueFromSetOfStatistics(statistics1);
			Value value2 = ConversionUtils.getValueFromSetOfStatistics(statistics2);

			// Create mutation
			Mutation m1A = new Mutation(key1A.getRow());
			m1A.put(key1A.getColumnFamily(), key1A.getColumnQualifier(), new ColumnVisibility(key1A.getColumnVisibility()), key1A.getTimestamp(), value1);
			Mutation m1B = new Mutation(key1B.getRow());
			m1B.put(key1B.getColumnFamily(), key1B.getColumnQualifier(), new ColumnVisibility(key1B.getColumnVisibility()), key1B.getTimestamp(), value1);
			Mutation m2A = new Mutation(key2A.getRow());
			m2A.put(key2A.getColumnFamily(), key2A.getColumnQualifier(), new ColumnVisibility(key2A.getColumnVisibility()), key2A.getTimestamp(), value2);
			Mutation m2B = new Mutation(key2B.getRow());
			m2B.put(key2B.getColumnFamily(), key2B.getColumnQualifier(), new ColumnVisibility(key2B.getColumnVisibility()), key2B.getTimestamp(), value2);

			// Write mutation
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			writer.addMutation(m1A);
			writer.addMutation(m1B);
			writer.addMutation(m2A);
			writer.addMutation(m2B);
			writer.close();

			// Read data back and check we get two identical edges
			Authorizations authorizations = new Authorizations(visibilityString);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			Entry<Key,Value> entry1 = it.next();
			Edge readEdge1 = ConversionUtils.getEdgeFromKey(entry1.getKey());
			SetOfStatistics readStatistics1 =  ConversionUtils.getSetOfStatisticsFromValue(entry1.getValue());
			Entry<Key,Value> entry2 = it.next();
			Edge readEdge2 = ConversionUtils.getEdgeFromKey(entry2.getKey());
			SetOfStatistics readStatistics2 =  ConversionUtils.getSetOfStatisticsFromValue(entry2.getValue());
			
			// Check no more entries
			if (it.hasNext()) {
				fail("Additional row found.");
			}

			// Check entries read are correct (i.e. come from edge 1)
			assertEquals(edge1, readEdge1);
			assertEquals(statistics1, readStatistics1);
			assertEquals(edge1, readEdge2);
			assertEquals(statistics1, readStatistics2);

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
	public void testVersioningIsOff() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		String visibilityString = "public";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

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

			// Remove the SetOfStatisticsCombiner iterator
			// (or else the identical keys we put will be combined meaning we can't
			// test whether the versioning is off).
			conn.tableOperations().removeIterator(tableName, "SetOfStatisticsCombiner", EnumSet.allOf(IteratorScope.class));
			
			// Create start and end dates for edge (from a week before the current date)
			Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
			sevenDaysBeforeCalendar.setTime(new Date(currentTime));
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
			Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
			sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
			Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();

			// Create edge
			Edge edge = new Edge("customer", "customer", "product", "P", "purchase", "instore", true, visibilityString, sevenDaysBefore, sixDaysBefore);

			// Create some SetOfStatistics
			SetOfStatistics statistics = new SetOfStatistics();
			statistics.addStatistic("count", new Count(1));

			// Accumulo keys
			Key key = ConversionUtils.getKeysFromEdge(edge).getFirst();
			Key key2 = new Key(key);
			key2.setTimestamp(key.getTimestamp() - 100L);

			// Accumulo values
			Value value1 = ConversionUtils.getValueFromSetOfStatistics(statistics);
			Value value2 = ConversionUtils.getValueFromSetOfStatistics(statistics);

			// Create mutation
			Mutation m1 = new Mutation(key.getRow());
			m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
			Mutation m2 = new Mutation(key.getRow());
			m2.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key2.getTimestamp(), value2);

			// Write mutation
			BatchWriter writer = conn.createBatchWriter(tableName, 1000000L, 1000L, 1);
			writer.addMutation(m1);
			writer.addMutation(m2);
			writer.close();

			// Read data back and check we get two edges
			Authorizations authorizations = new Authorizations(visibilityString);
			Scanner scanner = conn.createScanner(tableName, authorizations);
			Iterator<Entry<Key,Value>> it = scanner.iterator();
			Entry<Key,Value> entry = it.next();
			Edge readEdge = ConversionUtils.getEdgeFromKey(entry.getKey());
			SetOfStatistics readStatistics =  ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(edge, readEdge);
			assertEquals(statistics, readStatistics);
			entry = it.next();
			readEdge = ConversionUtils.getEdgeFromKey(entry.getKey());
			readStatistics =  ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
			assertEquals(edge, readEdge);
			assertEquals(statistics, readStatistics);

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
