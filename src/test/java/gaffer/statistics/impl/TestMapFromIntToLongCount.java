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
 * Basic tests for {@link MapFromIntToLongCount}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapFromIntToLongCount {

	@Test
	public void testWriteAndRead() {
		try {
			Map<Integer, Long> map1 = new HashMap<Integer, Long>();
			map1.put(2010, 1000000000L);
			map1.put(2011, 2000000000L);
			MapFromIntToLongCount moc1 = new MapFromIntToLongCount(map1);
			Map<Integer, Long> map2 = new HashMap<Integer, Long>();
			map2.put(2011, 1000000000L);
			map2.put(2012, 2000000000L);
			MapFromIntToLongCount moc2 = new MapFromIntToLongCount(map2);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			moc1.write(out);
			moc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapFromIntToLongCount stim3 = new MapFromIntToLongCount();
			stim3.readFields(in);
			MapFromIntToLongCount stim4 = new MapFromIntToLongCount();
			stim4.readFields(in);
			assertEquals(moc1, stim3);
			assertEquals(moc2, stim4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		Map<Integer, Long> map1 = new HashMap<Integer, Long>();
		map1.put(2010, 1000000000L);
		map1.put(2011, 2000000000L);
		MapFromIntToLongCount moc1 = new MapFromIntToLongCount(map1);
		Map<Integer, Long> map2 = new HashMap<Integer, Long>();
		map2.put(2011, 1000000000L);
		map2.put(2012, 2000000000L);
		MapFromIntToLongCount moc2 = new MapFromIntToLongCount(map2);
		
		Map<Integer, Long> expectedResult = new TreeMap<Integer, Long>();
		expectedResult.put(2010, 1000000000L);
		expectedResult.put(2011, 3000000000L);
		expectedResult.put(2012, 2000000000L);
		
		moc1.merge(moc2);
		
		assertEquals(expectedResult, moc1.getMap());
	}

	@Test
	public void testFailedMerge() {
		try {
			Map<Integer, Long> map1 = new HashMap<Integer, Long>();
			map1.put(2010, 1000000000L);
			map1.put(2011, 2000000000L);
			MapFromIntToLongCount moc1 = new MapFromIntToLongCount(map1);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			moc1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testAdd() {
		Map<Integer, Long> map = new HashMap<Integer, Long>();
		map.put(0xA, 1000000000L);
		map.put(0xB, 2000000000L);
		MapFromIntToLongCount moc = new MapFromIntToLongCount(map);
		moc.add(0xA, 5000000000L);
		moc.add(0xC, 10000000000L);
		
		Map<Integer, Long> expectedResult = new TreeMap<Integer, Long>();
		expectedResult.put(0xA, 6000000000L);
		expectedResult.put(0xB, 2000000000L);
		expectedResult.put(0xC, 10000000000L);
		MapFromIntToLongCount moc2 = new MapFromIntToLongCount(expectedResult);
		
		assertEquals(moc, moc2);
	}
}
