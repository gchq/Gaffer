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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

/**
 * Basic tests for {@link MapFromStringToSetOfStrings}.The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMapFromStringToSetOfStrings {

	@Test
	public void testWriteAndRead() {
		try {
			MapFromStringToSetOfStrings mfstsos1 = new MapFromStringToSetOfStrings();
			mfstsos1.add("A", "V1");
			mfstsos1.add("A", "V2");
			mfstsos1.add("B", "V3");
			MapFromStringToSetOfStrings mfstsos2 = new MapFromStringToSetOfStrings();
			mfstsos2.add("C", "V1");
			mfstsos2.add("D", "V2");
			mfstsos2.add("E", "V3");

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			mfstsos1.write(out);
			mfstsos2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MapFromStringToSetOfStrings mfstsos3 = new MapFromStringToSetOfStrings();
			mfstsos3.readFields(in);
			MapFromStringToSetOfStrings mfstsos4 = new MapFromStringToSetOfStrings();
			mfstsos4.readFields(in);
			assertEquals(mfstsos1, mfstsos3);
			assertEquals(mfstsos2, mfstsos4);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		MapFromStringToSetOfStrings mfstsos1 = new MapFromStringToSetOfStrings();
		mfstsos1.add("A", "V1");
		mfstsos1.add("A", "V2");
		mfstsos1.add("B", "V3");
		MapFromStringToSetOfStrings mfstsos2 = new MapFromStringToSetOfStrings();
		mfstsos2.add("A", "V1");
		mfstsos2.add("A", "V4");
		mfstsos2.add("E", "V3");

		Map<String, SortedSet<String>> expectedResult = new TreeMap<String, SortedSet<String>>();
		SortedSet<String> v1 = new TreeSet<String>();
		v1.add("V1");
		v1.add("V2");
		v1.add("V4");
		expectedResult.put("A", v1);
		SortedSet<String> v2 = new TreeSet<String>();
		v2.add("V3");
		expectedResult.put("B", v2);
		SortedSet<String> v3 = new TreeSet<String>();
		v3.add("V3");
		expectedResult.put("E", v2);

		mfstsos1.merge(mfstsos2);

		assertEquals(expectedResult, mfstsos1.getMap());
	}

	@Test
	public void testFailedMerge() {
		try {
			MapFromStringToSetOfStrings mfstsos1 = new MapFromStringToSetOfStrings();
			mfstsos1.add("A", "V1");
			mfstsos1.add("A", "V2");
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			mfstsos1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testAdd() {
		MapFromStringToSetOfStrings mfstsos1 = new MapFromStringToSetOfStrings();
		mfstsos1.add("A", "V1");
		mfstsos1.add("A", "V2");
		mfstsos1.add("B", "V3");

		SortedMap<String, SortedSet<String>> expectedResult = new TreeMap<String, SortedSet<String>>();
		SortedSet<String> s1 = new TreeSet<String>();
		s1.add("V1");
		s1.add("V2");
		expectedResult.put("A", s1);
		SortedSet<String> s2 = new TreeSet<String>();
		s2.add("V3");
		expectedResult.put("B", s2);

		assertEquals(expectedResult, mfstsos1.getMap());

		MapFromStringToSetOfStrings mfstsos2 = new MapFromStringToSetOfStrings(expectedResult);
		assertEquals(mfstsos1, mfstsos2);
	}

	@Test
	public void testClone() {
		// Create a MapFromStringToSetOfStrings
		MapFromStringToSetOfStrings mfstsos = new MapFromStringToSetOfStrings();
		mfstsos.add("A", "V1");
		mfstsos.add("A", "V2");
		mfstsos.add("B", "V3");
		// Clone it
		MapFromStringToSetOfStrings clone = mfstsos.clone();
		// Check that the two are equal
		assertEquals(mfstsos, clone);
		// Change the original
		mfstsos.add("C", "V4");
		// Check that the cloned one hasn't changed
		assertNotEquals(mfstsos, clone);
		MapFromStringToSetOfStrings mfstsos2 = new MapFromStringToSetOfStrings();
		mfstsos2.add("A", "V1");
		mfstsos2.add("A", "V2");
		mfstsos2.add("B", "V3");
		assertEquals(mfstsos2, clone);
	}
}
