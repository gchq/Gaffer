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
 * Basic tests for {@link MapFromIntToCount}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapFromIntToCount {

	@Test
	public void testWriteAndRead() {
		try {
			Map<Integer, Integer> map1 = new HashMap<Integer, Integer>();
			map1.put(2, 15);
			map1.put(7, 178);
			MapFromIntToCount mfitc1 = new MapFromIntToCount(map1);
			Map<Integer, Integer> map2 = new HashMap<Integer, Integer>();
			map2.put(2, 1);
			map2.put(7, 2);
			map2.put(2034, 12);
			MapFromIntToCount mfitc2 = new MapFromIntToCount(map2);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			mfitc1.write(out);
			mfitc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapFromIntToCount stim3 = new MapFromIntToCount();
			stim3.readFields(in);
			MapFromIntToCount stim4 = new MapFromIntToCount();
			stim4.readFields(in);
			assertEquals(mfitc1, stim3);
			assertEquals(mfitc2, stim4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		Map<Integer, Integer> map1 = new HashMap<Integer, Integer>();
		map1.put(2, 15);
		map1.put(7, 178);
		MapFromIntToCount mfitc1 = new MapFromIntToCount(map1);
		Map<Integer, Integer> map2 = new HashMap<Integer, Integer>();
		map2.put(2, 1);
		map2.put(7, 2);
		map2.put(2034, 12);
		MapFromIntToCount mfitc2 = new MapFromIntToCount(map2);
		
		Map<Integer, Integer> expectedResult = new TreeMap<Integer, Integer>();
		expectedResult.put(2, 16);
		expectedResult.put(7, 180);
		expectedResult.put(2034, 12);
		
		mfitc1.merge(mfitc2);
		
		assertEquals(expectedResult, mfitc1.getMap());
	}

	@Test
	public void testFailedMerge() {
		try {
			Map<Integer, Integer> map1 = new HashMap<Integer, Integer>();
			map1.put(2, 15);
			map1.put(7, 178);
			MapFromIntToCount mfitc1 = new MapFromIntToCount(map1);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			mfitc1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testAdd() {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		map.put(1, 2);
		map.put(5, 10);
		MapFromIntToCount mfitc1 = new MapFromIntToCount(map);
		mfitc1.add(1, 5);
		mfitc1.add(6, 20);
		
		Map<Integer, Integer> expectedResult = new TreeMap<Integer, Integer>();
		expectedResult.put(1, 7);
		expectedResult.put(5, 10);
		expectedResult.put(6, 20);
		MapFromIntToCount mfitc2 = new MapFromIntToCount(expectedResult);
		
		assertEquals(mfitc1, mfitc2);
	}
	
}
