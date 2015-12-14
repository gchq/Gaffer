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
 * Basic tests for {@link MapOfCounts}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapOfCounts {

	@Test
	public void testWriteAndRead() {
		try {
			Map<String, Integer> map1 = new HashMap<String, Integer>();
			map1.put("2010", 1);
			map1.put("2011", 2);
			MapOfCounts moc1 = new MapOfCounts(map1);
			Map<String, Integer> map2 = new HashMap<String, Integer>();
			map2.put("2011", 1);
			map2.put("2012", 2);
			MapOfCounts moc2 = new MapOfCounts(map2);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			moc1.write(out);
			moc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapOfCounts stim3 = new MapOfCounts();
			stim3.readFields(in);
			MapOfCounts stim4 = new MapOfCounts();
			stim4.readFields(in);
			assertEquals(moc1, stim3);
			assertEquals(moc2, stim4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		Map<String, Integer> map1 = new HashMap<String, Integer>();
		map1.put("2010", 1);
		map1.put("2011", 2);
		MapOfCounts moc1 = new MapOfCounts(map1);
		Map<String, Integer> map2 = new HashMap<String, Integer>();
		map2.put("2011", 1);
		map2.put("2012", 2);
		MapOfCounts moc2 = new MapOfCounts(map2);
		
		Map<String, Integer> expectedResult = new TreeMap<String, Integer>();
		expectedResult.put("2010", 1);
		expectedResult.put("2011", 3);
		expectedResult.put("2012", 2);
		
		moc1.merge(moc2);
		
		assertEquals(expectedResult, moc1.getMap());
	}

	@Test
	public void testFailedMerge() {
		try {
			Map<String, Integer> map1 = new HashMap<String, Integer>();
			map1.put("2010", 1);
			map1.put("2011", 2);
			MapOfCounts moc1 = new MapOfCounts(map1);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			moc1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testAdd() {
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("A", 1);
		map.put("B", 2);
		MapOfCounts moc = new MapOfCounts(map);
		moc.add("A", 5);
		moc.add("C", 10);
		
		Map<String, Integer> expectedResult = new TreeMap<String, Integer>();
		expectedResult.put("A", 6);
		expectedResult.put("B", 2);
		expectedResult.put("C", 10);
		MapOfCounts moc2 = new MapOfCounts(expectedResult);
		
		assertEquals(moc, moc2);
	}
}
