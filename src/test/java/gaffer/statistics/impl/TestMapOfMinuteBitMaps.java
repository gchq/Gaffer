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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Basic tests for {@link MapOfMinuteBitMaps}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapOfMinuteBitMaps {

	@Test
	public void testWriteAndRead() {
		try {
			Map<String, MinuteBitMap> map1 = new HashMap<String, MinuteBitMap>();
			map1.put("A", new MinuteBitMap(new Date(1241244)));
			map1.put("B", new MinuteBitMap(new Date(1000000)));
			MapOfMinuteBitMaps momb1 = new MapOfMinuteBitMaps(map1);

			MapOfMinuteBitMaps momb2 = new MapOfMinuteBitMaps();
			momb2.add("B", new Date(1100000));
			momb2.add("C", new Date(9999999));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			momb1.write(out);
			momb2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapOfMinuteBitMaps omomb1 = new MapOfMinuteBitMaps();
			omomb1.readFields(in);
			MapOfMinuteBitMaps omomb2 = new MapOfMinuteBitMaps();
			omomb2.readFields(in);
			assertEquals(momb1, omomb1);
			assertEquals(momb2, omomb2);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		Map<String, MinuteBitMap> map1 = new HashMap<String, MinuteBitMap>();
		map1.put("A", new MinuteBitMap(new Date(1241244)));
		map1.put("B", new MinuteBitMap(new Date(1000000)));
		MapOfMinuteBitMaps momb1 = new MapOfMinuteBitMaps(map1);

		MapOfMinuteBitMaps momb2 = new MapOfMinuteBitMaps();
		momb2.add("B", new Date(1100000));
		momb2.add("C", new Date(9999999));

		MapOfMinuteBitMaps expectedResult = new MapOfMinuteBitMaps();
		expectedResult.add("A", new MinuteBitMap(new Date(1241244)));
		MinuteBitMap bitmap = new MinuteBitMap(new Date(1000000));
		bitmap.add(new Date(1100000));
		expectedResult.add("B", bitmap);
		expectedResult.add("C", new MinuteBitMap(new Date(9999999)));

		momb1.merge(momb2);

		assertEquals(expectedResult, momb1);
	}

	@Test
	public void testFailedMerge() {
		try {
			Map<String, MinuteBitMap> map1 = new HashMap<String, MinuteBitMap>();
			map1.put("A", new MinuteBitMap(new Date(1241244)));
			map1.put("B", new MinuteBitMap(new Date(1000000)));
			MapOfMinuteBitMaps momb1 = new MapOfMinuteBitMaps(map1);

			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			momb1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}
}
