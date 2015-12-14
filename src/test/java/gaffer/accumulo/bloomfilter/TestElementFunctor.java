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
package gaffer.accumulo.bloomfilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import gaffer.Pair;
import gaffer.accumulo.Constants;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

/**
 * Tests the transform methods in {@link ElementFunctor}. Includes test for the way
 * this is used to get all information about a {@link TypeValue}.
 */
public class TestElementFunctor {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}
	private final static ElementFunctor elementFunctor = new ElementFunctor();
	
	@Test
	public void testTransformRangeEntity() {
		try {
			// Create Range formed from one entity and test
			Entity entity1 = new Entity("type", "value", "infoType", "infoSubType", "visibility",
					DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			String key1 = ConversionUtils.getRowKeyFromEntity(entity1);
			Range range1 = new Range(key1, true, key1, true);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(key1.getBytes());
			assertTrue(elementFunctor.transform(range1).equals(expectedBloomKey1));

			// Create Range formed from two entities and test - should get null
			Entity entity2 = new Entity("type2", "value2", "infoType", "infoSubType", "visibility",
					DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			String key2 = ConversionUtils.getRowKeyFromEntity(entity2);
			Range range2 = new Range(key1, true, key2, true);
			assertNull(elementFunctor.transform(range2));
		} catch (ParseException e) {
			fail("ParseException " + e);
		}
	}

	@Test
	public void testTransformKeyEntity() {
		try {
			// Create Key formed from entity and test
			Entity entity1 = new Entity("type", "value", "infoType", "infoSubType", "visibility",
					DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			org.apache.accumulo.core.data.Key key1 = ConversionUtils.getKeyFromEntity(entity1);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(key1.getRowData().toArray());
			assertEquals(expectedBloomKey1, elementFunctor.transform(key1));
		} catch (ParseException e) {
			fail("ParseException " + e);
		}
	}

	@Test
	public void testTransformRangeEdge() {
		try {
			// Create Range formed from one edge and test
			Edge edge1 = new Edge("srcType", "srcValue", "dstType", "dstValue", "edgeType", "edgeSubType", true,
					"visibility", DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			Pair<org.apache.accumulo.core.data.Key> keys = ConversionUtils.getKeysFromEdge(edge1);
			Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(keys.getFirst()));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range1));
			Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
			org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(keys.getSecond()));
			assertEquals(expectedBloomKey2, elementFunctor.transform(range2));
			
			// Create Range formed from two keys and test - should get null
			Range range3 = new Range(keys.getSecond().getRow(), true, keys.getFirst().getRow(), true);
			assertNull(elementFunctor.transform(range3));
		} catch (ParseException e) {
			fail("ParseException " + e);
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformKeyEdge() {
		try {
			// Create Key formed from edge and test
			Edge edge1 = new Edge("srcType", "srcValue", "dstType", "dstValue", "edgeType", "edgeSubType", true,
					"visibility", DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			Pair<org.apache.accumulo.core.data.Key> keys = ConversionUtils.getKeysFromEdge(edge1);
			Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(keys.getFirst()));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range1));
			Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
			org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(keys.getSecond()));
			assertEquals(expectedBloomKey2, elementFunctor.transform(range2));
		} catch (ParseException e) {
			fail("ParseException " + e);
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeFromEntityToEntityAndSomeEdges() {
		try {
			// Create entity
			Entity entity = new Entity("type", "value", "infoType", "infoSubType", "visibility",
					DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			//		String key1 = ConversionUtils.getRowKeyFromEntity(entity1);
			Key key1 = ConversionUtils.getKeyFromEntity(entity);

			// Create edge from that entity
			Edge edge = new Edge("type", "value", "dstType", "dstValue", "edgeType", "edgeSubType", true,
					"visibility", DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			//		String key2 = ConversionUtils.getRowKeysFromEdge(edge).getFirst();
			Key key2 = ConversionUtils.getKeysFromEdge(edge).getFirst();

			// Create range from entity to edge inclusive
			Range range = new Range(key1.getRow(), true, key2.getRow(), true);

			// Check don't get null Bloom key
			assertNotNull(elementFunctor.transform(range));
			
			// Check get correct Bloom key
			org.apache.hadoop.util.bloom.Key expectedBloomKey = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key1));
			assertEquals(expectedBloomKey, elementFunctor.transform(range));
		} catch (ParseException e) {
			fail("ParseException " + e);
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenAskingForEverythingAboutATypeValue() {
		try {
			// Create TypeValue
			TypeValue typeValue = new TypeValue("type", "value");
			String rowKey = ConversionUtils.getRowKeyFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			Key key = new Key(rowKey);
			Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
			assertNotNull(elementFunctor.transform(range));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range));
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenAskingForEverythingAboutATypeValueContainingDelimiter() {
		try {
			// Create TypeValue
			TypeValue typeValue = new TypeValue("type" + Constants.DELIMITER, "value" + Constants.DELIMITER + "more_value" + Constants.DELIMITER);
			String rowKey = ConversionUtils.getRowKeyFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			Key key = new Key(rowKey);
			Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
			assertNotNull(elementFunctor.transform(range));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range));
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenAskingForEverythingAboutATypeValueContainingDelimiterPlusOne() {
		try {
			// Create TypeValue
			TypeValue typeValue = new TypeValue("type" + Constants.DELIMITER_PLUS_ONE, "value" + Constants.DELIMITER_PLUS_ONE + "more_value" + Constants.DELIMITER_PLUS_ONE);
			String rowKey = ConversionUtils.getRowKeyFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			Key key = new Key(rowKey);
			Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
			assertNotNull(elementFunctor.transform(range));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range));
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenUsingRangeDotExact() {
		try {
			// Create TypeValue
			TypeValue typeValue = new TypeValue("type", "value");
			String rowKey = ConversionUtils.getRowKeyFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			Key key = new Key(rowKey);
			Range range = Range.exact(rowKey);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
			assertNotNull(elementFunctor.transform(range));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range));
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenUsingRangeDotExactContainingDelimiter() {
		try {
			// Create TypeValue
			TypeValue typeValue = new TypeValue("type" + Constants.DELIMITER, "value" + Constants.DELIMITER);
			String rowKey = ConversionUtils.getRowKeyFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			Key key = new Key(rowKey);
			Range range = Range.exact(rowKey);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
			assertNotNull(elementFunctor.transform(range));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range));
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenUsingRangeDotExactContainingDelimiterPlusOne() {
		try {
			// Create TypeValue
			TypeValue typeValue = new TypeValue("type" + Constants.DELIMITER_PLUS_ONE, "value" + Constants.DELIMITER_PLUS_ONE + "more_value" + Constants.DELIMITER_PLUS_ONE);
			String rowKey = ConversionUtils.getRowKeyFromTypeAndValue(typeValue.getType(), typeValue.getValue());
			Key key = new Key(rowKey);
			Range range = Range.exact(rowKey);
			org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ConversionUtils.getFirstTypeValue(key));
			assertNotNull(elementFunctor.transform(range));
			assertEquals(expectedBloomKey1, elementFunctor.transform(range));
		} catch (IOException e) {
			fail("IOException " + e);
		}
	}

	@Test
	public void testTransformRangeWhenRangeHasUnspecifiedStartOrEndKey() {
		try {
			// Create Range with unspecified start key and test - should get null
			Edge edge1 = new Edge("srcType", "srcValue", "dstType", "dstValue", "edgeType", "edgeSubType", true,
					"visibility", DATE_FORMAT.parse("20140101000000"), DATE_FORMAT.parse("20140102000000"));
			Pair<org.apache.accumulo.core.data.Key> keys = ConversionUtils.getKeysFromEdge(edge1);
			Range range1 = new Range(null, true, keys.getFirst().getRow(), true);
			assertNull(elementFunctor.transform(range1));

			// Create Range with unspecified end key and test - should get null
			Range range2 = new Range(keys.getFirst().getRow(), true, null, true);
			assertNull(elementFunctor.transform(range2));
		} catch (ParseException e) {
			fail("ParseException " + e);
		}
	}

	@Test
	public void testTransformKeyWhenKeyIsNotEntityOrEdge() {
		// Create arbitrary key
		org.apache.accumulo.core.data.Key key = new org.apache.accumulo.core.data.Key("Blah");
		assertNull(elementFunctor.transform(key));
	}

	@Test
	public void testTransformRangeWhenKeyIsNotEntityOrEdge() {
		// Create arbitrary range
		Range range = new Range("Blah", true, "MoreBlah", true);
		assertNull(elementFunctor.transform(range));
	}

}