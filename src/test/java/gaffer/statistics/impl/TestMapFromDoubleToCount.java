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
 * Basic tests for {@link MapFromDoubleToCount}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapFromDoubleToCount {

	@Test
	public void testWriteAndRead() {
		try {
			Map<Double, Integer> map1 = new HashMap<Double, Integer>();
			map1.put(2.0d, 15);
			map1.put(7.2d, 178);
			MapFromDoubleToCount mfdtc1 = new MapFromDoubleToCount(map1);
			Map<Double, Integer> map2 = new HashMap<Double, Integer>();
			map2.put(2.0d, 1);
			map2.put(7.2d, 2);
			map2.put(2034.8d, 12);
			MapFromDoubleToCount mfdtc2 = new MapFromDoubleToCount(map2);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			mfdtc1.write(out);
			mfdtc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapFromDoubleToCount mfdtc3 = new MapFromDoubleToCount();
			mfdtc3.readFields(in);
			MapFromDoubleToCount mfdtc4 = new MapFromDoubleToCount();
			mfdtc4.readFields(in);
			assertEquals(mfdtc1, mfdtc3);
			assertEquals(mfdtc2, mfdtc4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		Map<Double, Integer> map1 = new HashMap<Double, Integer>();
		map1.put(2.0d, 15);
		map1.put(7.2d, 178);
		MapFromDoubleToCount mfdtc1 = new MapFromDoubleToCount(map1);
		Map<Double, Integer> map2 = new HashMap<Double, Integer>();
		map2.put(2.0d, 1);
		map2.put(7.2d, 2);
		map2.put(2034.8d, 12);
		MapFromDoubleToCount mfdtc2 = new MapFromDoubleToCount(map2);
		
		Map<Double, Integer> expectedResult = new TreeMap<Double, Integer>();
		expectedResult.put(2.0d, 16);
		expectedResult.put(7.2d, 180);
		expectedResult.put(2034.8d, 12);
		
		mfdtc1.merge(mfdtc2);
		
		assertEquals(expectedResult, mfdtc1.getMap());
	}

	@Test
	public void testFailedMerge() {
		try {
			Map<Double, Integer> map1 = new HashMap<Double, Integer>();
			map1.put(2.0d, 15);
			map1.put(7.2d, 178);
			MapFromDoubleToCount mfitc1 = new MapFromDoubleToCount(map1);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			mfitc1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testAdd() {
		Map<Double, Integer> map = new HashMap<Double, Integer>();
		map.put(1.0d, 2);
		map.put(5.6d, 10);
		MapFromDoubleToCount mfitc1 = new MapFromDoubleToCount(map);
		mfitc1.add(1.0d, 5);
		mfitc1.add(6.3d, 20);
		
		Map<Double, Integer> expectedResult = new TreeMap<Double, Integer>();
		expectedResult.put(1.0d, 7);
		expectedResult.put(5.6d, 10);
		expectedResult.put(6.3d, 20);
		MapFromDoubleToCount mfitc2 = new MapFromDoubleToCount(expectedResult);
		
		assertEquals(mfitc1, mfitc2);
	}
	
}
