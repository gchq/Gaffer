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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Unit tests for {@link CappedSetOfStrings}.
 */
public class TestCappedSetOfStrings {

	@Test
	public void testFull() {
		CappedSetOfStrings s1 = new CappedSetOfStrings(3);
		s1.addString("A");
		s1.addString("B");
		s1.addString("B");
		s1.addString("C");
		assertFalse(s1.isFull());
		s1.addString("D");
		assertTrue(s1.isFull());
	}
	
	@Test
	public void testExceptionWhenSetIsBiggerThanMaxSize() {
		Set<String> strings = new HashSet<String>();
		strings.add("A");
		strings.add("B");
		strings.add("C");
		strings.add("D");
		try {
			@SuppressWarnings("unused")
			CappedSetOfStrings s1 = new CappedSetOfStrings(3, strings);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown");
	}
	
	@Test
	public void testEquals() {
		// Test equals on two identical sets
		CappedSetOfStrings s1 = new CappedSetOfStrings(10);
		s1.addString("A");
		s1.addString("B");
		CappedSetOfStrings s2 = new CappedSetOfStrings(10);
		s2.addString("A");
		s2.addString("B");
		assertEquals(s1, s2);

		// Test equals on two different sets
		s1 = new CappedSetOfStrings(10);
		s1.addString("A");
		s1.addString("B");
		s2 = new CappedSetOfStrings(10);
		s2.addString("A");
		s2.addString("C");
		assertNotEquals(s1, s2);

		// Test equals when different maxSizes
		s1 = new CappedSetOfStrings(10);
		s1.addString("A");
		s2 = new CappedSetOfStrings(20);
		s2.addString("B");
		assertNotEquals(s1, s2);
		
		// Test equals when one full and the other isn't
		s1 = new CappedSetOfStrings(1);
		s1.addString("A");
		s1.addString("B");
		assertTrue(s1.isFull());
		s2 = new CappedSetOfStrings(2);
		s2.addString("B");
		assertFalse(s2.isFull());
		assertNotEquals(s1, s2);
	}

	@Test
	public void testWriteAndRead() {
		try {
			CappedSetOfStrings s1 = new CappedSetOfStrings(10);
			s1.addString("A");
			s1.addString("B");
			CappedSetOfStrings s2 = new CappedSetOfStrings(1);
			s2.addString("C");
			s2.addString("D");

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			s1.write(out);
			s2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			CappedSetOfStrings read1 = new CappedSetOfStrings();
			read1.readFields(in);
			CappedSetOfStrings read2 = new CappedSetOfStrings();
			read2.readFields(in);

			assertEquals(s1, read1);
			assertEquals(s2, read2);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		CappedSetOfStrings s1 = new CappedSetOfStrings(10);
		s1.addString("A");
		s1.addString("B");
		CappedSetOfStrings s2 = new CappedSetOfStrings(10);
		s2.addString("C");
		s2.addString("D");

		// Merge and check merged result is correct
		s1.merge(s2);

		Set<String> expected = new HashSet<String>();
		expected.add("A");
		expected.add("B");
		expected.add("C");
		expected.add("D");
		CappedSetOfStrings expectedResult = new CappedSetOfStrings(10, expected);
		assertEquals(expectedResult, s1);

		// Check s2 hasn't changed
		expected.clear();
		expected.add("C");
		expected.add("D");
		expectedResult = new CappedSetOfStrings(10, expected);
		assertEquals(expectedResult, s2);
	}

	@Test
	public void testMergeWhenOneIsFull() {
		CappedSetOfStrings s1 = new CappedSetOfStrings(2);
		s1.addString("A");
		CappedSetOfStrings s2 = new CappedSetOfStrings(2);
		s2.addString("B");
		s2.addString("C");
		s2.addString("D");
		s1.merge(s2);
		assertTrue(s1.isFull());
	}
	
	@Test
	public void testMergeFailsWhenDifferentMaxSizes() {
		CappedSetOfStrings s1 = new CappedSetOfStrings(2);
		s1.addString("A");
		CappedSetOfStrings s2 = new CappedSetOfStrings(3);
		s2.addString("B");
		try {
			s1.merge(s2);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown");
	}
	
	@Test
	public void testCloneOfFullSet() {
		CappedSetOfStrings s1 = new CappedSetOfStrings(2);
		s1.addString("A");
		s1.addString("B");
		s1.addString("C");
		assertTrue(s1.isFull());
		CappedSetOfStrings s2 = s1.clone();
		assertTrue(s2.isFull());
		assertEquals(s2.getSize(), s1.getSize());
		assertEquals(s2.getMaxSize(),s1.getMaxSize());
	}
	
	@Test
	public void testConstructorClonesSet() {
		// Create CappedSetOfStrings from Set
		Set<String> s1 = new HashSet<String>();
		s1.add("A");
		s1.add("B");
		CappedSetOfStrings s = new CappedSetOfStrings(10, s1);

		// Change original set
		s1.add("C");

		// Check CappedSetOfStrings hasn't changed
		CappedSetOfStrings s2 = new CappedSetOfStrings(10);
		s2.addString("A");
		s2.addString("B");
		assertEquals(s, s2);

		// Add to CappedSetOfStrings
		s1.remove("C");
		s.addString("X");

		// Check that Set hasn't changed
		Set<String> expected = new HashSet<String>();
		expected.add("A");
		expected.add("B");
		assertEquals(expected, s1);
	}

}
