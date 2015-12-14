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

import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link IntSet}. Contains tests for: adding ints,
 * writing and reading, cloning, merging, etc.
 */
public class TestIntSet {

	@Test
	public void testContains() {
		// Add one int
		IntSet intSet1 = new IntSet();
		intSet1.add(178);
		assertTrue(intSet1.contains(178));
		assertFalse(intSet1.contains(179));
		assertFalse(intSet1.contains(0));
		assertFalse(intSet1.contains(1000));

		// Add multiple ints
		IntSet intSet2 = new IntSet();
		intSet2.add(100);
		intSet2.add(12345);
		intSet2.add(-7654);
		assertTrue(intSet2.contains(100));
		assertTrue(intSet2.contains(12345));
		assertTrue(intSet2.contains(-7654));
		assertFalse(intSet2.contains(101));
		assertFalse(intSet2.contains(-989));
		assertFalse(intSet2.contains(12346));
	}

	@Test
	public void testAddTwice() {
		IntSet intSet = new IntSet();
		intSet.add(1);
		intSet.add(1);
		Set<Integer> ints = new HashSet<Integer>();
		Iterator<Integer> it = intSet.getIterator();
		int count = 0;
		while (it.hasNext()) {
			count++;
			ints.add(it.next());
		}
		assertEquals(1, count);
		assertEquals(Collections.singleton(1), ints);
	}

	@Test
	public void testWriteRead() {
		try {
			IntSet intSet1 = new IntSet();
			intSet1.add(100);
			intSet1.add(12345);
			intSet1.add(-7654);
			IntSet intSet2 = new IntSet();
			intSet2.add(1000000);
			intSet2.add(-9);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			intSet1.write(out);
			intSet2.write(out);
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			IntSet intSet3 = new IntSet();
			intSet3.readFields(in);
			IntSet intSet4 = new IntSet();
			intSet4.readFields(in);
			assertEquals(intSet1, intSet3);
			assertEquals(intSet2, intSet4);
		} catch (IOException e) {
			fail("IOException: " + e);
		}
	}

	@Test
	public void testMerge() {
		// Merge an identical IntSet in
		IntSet intSet1 = new IntSet();
		intSet1.add(100);
		intSet1.add(989);
		IntSet intSet2 = new IntSet();
		intSet2.add(100);
		intSet2.add(989);
		intSet1.merge(intSet2);
		assertEquals(intSet1, intSet2);

		// Merge two different IntSets
		IntSet intSet3 = new IntSet();
		intSet3.add(100);
		intSet3.add(989);
		IntSet intSet4 = new IntSet();
		intSet4.add(1);
		intSet4.add(98900);
		intSet3.merge(intSet4);
		IntSet expected = new IntSet();
		expected.add(100, 989, 1, 98900);
		assertEquals(expected, intSet3);

		// Check intSet4 hasn't changed
		IntSet intSet5 = new IntSet();
		intSet5.add(1, 98900);
		assertEquals(intSet5, intSet4);
	}

	@Test
	public void testClone() {
		// Create an IntSet
		IntSet intSet = new IntSet();
		intSet.add(100, 765);
		// Clone it
		IntSet cloneOfIntSet = intSet.clone();
		// Check that the two are equal
		assertEquals(intSet, cloneOfIntSet);
		// Change the original
		intSet.add(12345, 98765);
		// Check that the cloned one hasn't changed
		assertNotEquals(intSet, cloneOfIntSet);
		IntSet expected = new IntSet();
		expected.add(100, 765);
		assertEquals(expected, cloneOfIntSet);
	}

	@Test
	public void testHashAndEquals() {
		// Create an IntSet
		IntSet intSet = new IntSet();
		intSet.add(1, 7, 190);
		// Clone it
		IntSet cloneOfIntSet = intSet.clone();
		// Check that the two are equal
		assertEquals(intSet, cloneOfIntSet);
		// Check that they hash to the same value
		assertEquals(intSet.hashCode(), cloneOfIntSet.hashCode());

		// Create another one that's identical to intSet
		IntSet intSet2 = new IntSet();
		intSet2.add(1);
		intSet2.add(7);
		intSet2.add(190);
		// Check that the two are equal
		assertEquals(intSet, intSet2);
		// Check that they hash to the same value
		assertEquals(intSet.hashCode(), intSet2.hashCode());
	}

}
