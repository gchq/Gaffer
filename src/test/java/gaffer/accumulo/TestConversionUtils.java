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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import gaffer.Pair;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import gaffer.statistics.impl.FirstSeen;
import gaffer.statistics.impl.LastSeen;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Tests of some of the utilities that convert from Entity and Edges to Accumulo keys.
 * In particular includes tests of conversion from {@link Entity} and {@link Edge}
 * to Accumulo {@link Key} when the types and values include the {@link Constants#DELIMITER}
 * character that is used by {@link ConversionUtils} to delimit types and values within
 * Accumulo.
 */
public class TestConversionUtils {

	private static final String visibility = "public";

	@Test
	public void testEntityToKeyConversion() {
		// Create start and end dates for entity
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create entity
		Entity originalEntity = new Entity("customer", "A", "purchase", "instore", visibility, startDate, endDate);

		// Convert entity to key
		Key key = ConversionUtils.getKeyFromEntity(originalEntity);

		try {
			// Convert key back to entity
			Entity derivedEntity = ConversionUtils.getEntityFromKey(key);
			// Check
			assertEquals(originalEntity, derivedEntity);
		} catch (IOException e) {
			fail("IOException converting key to entity: " + e);
		}
	}

	@Test
	public void testEntityToKeyConversionWithDelimiterInTypesAndValues() {
		// Create start and end dates for entity
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create entity with Constants.DELIMITER character in type and value
		String entityType = "blah" + Constants.DELIMITER;
		String entityvalue = "more_blah" + Constants.DELIMITER + Constants.DELIMITER + "_more_blah";
		String infoType = "abc" + Constants.DELIMITER;
		String infoSubtype = "def" + Constants.DELIMITER + "ghi";
		Entity originalEntity = new Entity(entityType, entityvalue, infoType, infoSubtype, visibility, startDate, endDate);

		// Convert entity to key
		Key key = ConversionUtils.getKeyFromEntity(originalEntity);

		try {
			// Convert key back to entity
			Entity derivedEntity = ConversionUtils.getEntityFromKey(key);
			// Check
			assertEquals(originalEntity, derivedEntity);
		} catch (IOException e) {
			fail("IOException converting key to entity: " + e);
		}
	}

	@Test
	public void testEdgeToKeyConversion() {
		// Create start and end dates for edge
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create directed edge
		Edge originalEdge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility, startDate, endDate);

		try {
			// Convert edge to keys
			Pair<Key> pair = ConversionUtils.getKeysFromEdge(originalEdge);
			Key key1 = pair.getFirst();
			Key key2 = pair.getSecond();

			// Create edge from key and check equals original edge - key1
			Edge derivedEdge1 = ConversionUtils.getEdgeFromKey(key1);
			assertEquals(originalEdge, derivedEdge1);

			// Create edge from key and check equals original edge - key2
			Edge derivedEdge2 = ConversionUtils.getEdgeFromKey(key2);
			assertEquals(originalEdge, derivedEdge2);

			// Repeat for undirected edge
			originalEdge = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibility, startDate, endDate);
			pair = ConversionUtils.getKeysFromEdge(originalEdge);
			key1 = pair.getFirst();
			key2 = pair.getSecond();
			derivedEdge1 = ConversionUtils.getEdgeFromKey(key1);
			assertEquals(originalEdge, derivedEdge1);
			derivedEdge2 = ConversionUtils.getEdgeFromKey(key2);
			assertEquals(originalEdge, derivedEdge2);
		} catch (IOException e) {
			fail("IOException converting key to edge: " + e);
		}
	}

	@Test
	public void testEdgeToKeyConversionWithDelimiterInTypesAndValues() {
		// Create start and end dates for edge
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create directed edge
		String sourceType = "blah" + Constants.DELIMITER;
		String sourceValue = "more_blah" + Constants.DELIMITER + Constants.DELIMITER + "_more_blah";
		String destinationType = "blah2" + Constants.DELIMITER;
		String destinationValue = "more_blah2" + Constants.DELIMITER + Constants.DELIMITER + "_more_blah2";
		String edgeType = "abc" + Constants.DELIMITER;
		String edgeSubtype = "def" + Constants.DELIMITER + "ghi";
		Edge originalEdge = new Edge(sourceType, sourceValue, destinationType, destinationValue, edgeType, edgeSubtype, true, visibility, startDate, endDate);

		try {
			// Convert edge to keys
			Pair<Key> pair = ConversionUtils.getKeysFromEdge(originalEdge);
			Key key1 = pair.getFirst();
			Key key2 = pair.getSecond();

			// Create edge from key and check equals original edge - key1
			Edge derivedEdge1 = ConversionUtils.getEdgeFromKey(key1);
			assertEquals(originalEdge, derivedEdge1);

			// Create edge from key and check equals original edge - key2
			Edge derivedEdge2 = ConversionUtils.getEdgeFromKey(key2);
			assertEquals(originalEdge, derivedEdge2);

			// Repeat for undirected edge
			originalEdge = new Edge(sourceType, sourceValue, destinationType, destinationValue, edgeType, edgeSubtype, false, visibility, startDate, endDate);
			pair = ConversionUtils.getKeysFromEdge(originalEdge);
			key1 = pair.getFirst();
			key2 = pair.getSecond();
			derivedEdge1 = ConversionUtils.getEdgeFromKey(key1);
			assertEquals(originalEdge, derivedEdge1);
			derivedEdge2 = ConversionUtils.getEdgeFromKey(key2);
			assertEquals(originalEdge, derivedEdge2);
		} catch (IOException e) {
			fail("IOException converting key to edge: " + e);
		}
	}

	@Test
	public void testEdgeToKeyConversionWhenEdgeIsSelfEdge() {
		// Create start and end dates for edge
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create directed edge where the source and destination are identical
		String type = "customer";
		String value = "A";
		Edge originalEdge = new Edge(type, value, type, value, "purchase", "instore", true, visibility, startDate, endDate);

		try {
			// Convert edge to keys
			Pair<Key> pair = ConversionUtils.getKeysFromEdge(originalEdge);
			Key key1 = pair.getFirst();
			Key key2 = pair.getSecond();

			// Create edge from key and check equals original edge - key1
			Edge derivedEdge1 = ConversionUtils.getEdgeFromKey(key1);
			assertEquals(originalEdge, derivedEdge1);

			// Second key should be null
			assertNull(key2);

			// Repeat for undirected edge
			originalEdge = new Edge(type, value, type, value, "purchase", "instore", false, visibility, startDate, endDate);
			pair = ConversionUtils.getKeysFromEdge(originalEdge);
			key1 = pair.getFirst();
			key2 = pair.getSecond();
			derivedEdge1 = ConversionUtils.getEdgeFromKey(key1);
			assertEquals(originalEdge, derivedEdge1);
			assertNull(key2);
		} catch (IOException e) {
			fail("IOException converting key to edge: " + e);
		}
	}


	@Test
	public void testGetIOException() {
		// Create start and end dates for edge
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Invalid rowkey
		Text rowKey = new Text("customer" + Constants.DELIMITER + "A");

		// Column family is edge type and subtype
		Text columnFamily = new Text("purchase" + Constants.DELIMITER + "instore");

		// Column qualifier is start and end dates (written as longs)
		Text columnQualifier = new Text("" + startDate.getTime() + Constants.DELIMITER + endDate.getTime());

		// Column visibility
		ColumnVisibility visibilityCV = new ColumnVisibility(visibility);

		// Timestamp - equal to the time of the end date of the edge
		long timestamp = endDate.getTime();

		// Create key
		Key key = new Key(rowKey, columnFamily, columnQualifier, visibilityCV, timestamp);
		try {
			ConversionUtils.getEdgeFromKey(key);
		} catch (IOException e) {
			return;
		}
		fail("IOException should have been thrown whilst getting edge from invalid key.");
	}

	@Test
	public void testDoesKeyRepresentEntity() {
		// Create start and end dates for entity
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create entity
		Entity originalEntity = new Entity("customer", "A", "product", "instore", visibility, startDate, endDate);

		// Get key from Entity
		Key key = ConversionUtils.getKeyFromEntity(originalEntity);

		// Check get true
		try {
			assertTrue(ConversionUtils.doesKeyRepresentEntity(key.getRow().toString()));
		} catch (IOException e) {
			fail("IOException getting key from Entity");
		}

		// Create edge
		Edge edge = new Edge("customer", "A", "product", "P", "product", "instore", true, visibility, startDate, endDate);

		// Get row keys from Edge and check get false
		Pair<Key> keys = ConversionUtils.getKeysFromEdge(edge);
		try {
			assertFalse(ConversionUtils.doesKeyRepresentEntity(keys.getFirst().getRow().toString()));
			assertFalse(ConversionUtils.doesKeyRepresentEntity(keys.getSecond().getRow().toString()));
		} catch (IOException e) {
			fail("IOException getting key from Entity");
		}

		// Get row key from neither an Entity nor an Edge and check get IOException
		try {
			ConversionUtils.doesKeyRepresentEntity(new Key(new Text("dfsdfdfgdfg")).getRow().toString());
		} catch (IOException e) {
			return;
		}
		fail("IOException should have been thrown");
	}

	@Test
	public void testSetOfStatisticsToValueConversion() {
		// Create first seen and last seen dates
		Calendar firstSeenCalendar = new GregorianCalendar();
		firstSeenCalendar.clear();
		firstSeenCalendar.set(2014, Calendar.JANUARY, 1, 12, 34, 56);
		Date firstSeenDate = firstSeenCalendar.getTime();
		Calendar lastSeenCalendar = new GregorianCalendar();
		lastSeenCalendar.clear();
		lastSeenCalendar.set(2014, Calendar.JANUARY, 2, 14, 38, 59);
		Date lastSeenDate = lastSeenCalendar.getTime();

		// Create set of statistics
		SetOfStatistics originalStatistics = new SetOfStatistics();
		originalStatistics.addStatistic("firstSeen", new FirstSeen(firstSeenDate));
		originalStatistics.addStatistic("lastSeen", new LastSeen(lastSeenDate));
		originalStatistics.addStatistic("count", new Count(5));

		// Convert set of statistics to value
		Value value = ConversionUtils.getValueFromSetOfStatistics(originalStatistics);
		try {
			// Convert value to set of statistics
			SetOfStatistics derivedStatistics = ConversionUtils.getSetOfStatisticsFromValue(value);
			// Check that get same results back
			assertEquals(derivedStatistics, originalStatistics);
		} catch (IOException e) {
			fail("IOException converting Value to SetOfStatistics: " + e);
		}
	}

	@Test
	public void testGetFirstTypeValueWhenKeyRepresentsEntity() {
		// Create start and end dates for entity
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create entity
		Entity originalEntity = new Entity("customer", "A", "purchase", "instore", visibility, startDate, endDate);

		// Convert entity to key
		Key key = ConversionUtils.getKeyFromEntity(originalEntity);

		// Expected result
		byte[] expectedResult = new byte[]{'c', 'u', 's', 't', 'o', 'm', 'e', 'r', Constants.DELIMITER, 'A'};

		try {
			byte[] result = ConversionUtils.getFirstTypeValue(key);
			assertTrue(Arrays.equals(result, expectedResult));

		} catch (IOException e) {
			fail("IOException converting Accumulo key derived from entity to a byte array: " + e);
		}
	}

	@Test
	public void testGetFirstTypeValueWhenKeyRepresentsEdge() {
		// Create start and end dates for edge
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create directed edge
		Edge originalEdge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility, startDate, endDate);

		// Convert edge to keys
		Pair<Key> pair = ConversionUtils.getKeysFromEdge(originalEdge);
		Key key1 = pair.getFirst();
		Key key2 = pair.getSecond();

		// Expected results
		byte[] expectedResult1 = new byte[]{'c', 'u', 's', 't', 'o', 'm', 'e', 'r', Constants.DELIMITER, 'A'};
		byte[] expectedResult2 = new byte[]{'p', 'r', 'o', 'd', 'u', 'c', 't', Constants.DELIMITER, 'P'};

		try {
			byte[] result1 = ConversionUtils.getFirstTypeValue(key1);
			assertTrue(Arrays.equals(result1, expectedResult1));
			byte[] result2 = ConversionUtils.getFirstTypeValue(key2);
			assertTrue(Arrays.equals(result2, expectedResult2));

		} catch (IOException e) {
			fail("IOException converting Accumulo key derived from edge to a byte array: " + e);
		}
	}

}
