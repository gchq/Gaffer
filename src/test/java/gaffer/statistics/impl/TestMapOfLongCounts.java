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
package gaffer.statistics.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

/**
 * Basic tests for {@link MapOfLongCounts}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapOfLongCounts {

	@Test
	public void testWriteAndRead() {
		try {
			Map<String, Long> map1 = new HashMap<String, Long>();
			map1.put("2010", 1000000000L);
			map1.put("2011", 2000000000L);
			MapOfLongCounts moc1 = new MapOfLongCounts(map1);
			Map<String, Long> map2 = new HashMap<String, Long>();
			map2.put("2011", 1000000000L);
			map2.put("2012", 2000000000L);
			MapOfLongCounts moc2 = new MapOfLongCounts(map2);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			moc1.write(out);
			moc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapOfLongCounts stim3 = new MapOfLongCounts();
			stim3.readFields(in);
			MapOfLongCounts stim4 = new MapOfLongCounts();
			stim4.readFields(in);
			assertEquals(moc1, stim3);
			assertEquals(moc2, stim4);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		Map<String, Long> map1 = new HashMap<String, Long>();
		map1.put("2010", 1000000000L);
		map1.put("2011", 2000000000L);
		MapOfLongCounts moc1 = new MapOfLongCounts(map1);
		Map<String, Long> map2 = new HashMap<String, Long>();
		map2.put("2011", 1000000000L);
		map2.put("2012", 2000000000L);
		MapOfLongCounts moc2 = new MapOfLongCounts(map2);
		
		Map<String, Long> expectedResult = new TreeMap<String, Long>();
		expectedResult.put("2010", 1000000000L);
		expectedResult.put("2011", 3000000000L);
		expectedResult.put("2012", 2000000000L);
		
		moc1.merge(moc2);
		
		assertEquals(expectedResult, moc1.getMap());
	}

	@Test
	public void testFailedMerge() {
		try {
			Map<String, Long> map1 = new HashMap<String, Long>();
			map1.put("2010", 1000000000L);
			map1.put("2011", 2000000000L);
			MapOfLongCounts moc1 = new MapOfLongCounts(map1);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			moc1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testAdd() {
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("A", 1000000000L);
		map.put("B", 2000000000L);
		MapOfLongCounts moc = new MapOfLongCounts(map);
		moc.add("A", 5000000000L);
		moc.add("C", 10000000000L);
		
		Map<String, Long> expectedResult = new TreeMap<String, Long>();
		expectedResult.put("A", 6000000000L);
		expectedResult.put("B", 2000000000L);
		expectedResult.put("C", 10000000000L);
		MapOfLongCounts moc2 = new MapOfLongCounts(expectedResult);
		
		assertEquals(moc, moc2);
	}
}
