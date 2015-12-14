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
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Unit tests for {@link SetOfStrings}. There are tests for: equals, writing and reading, merging
 * and cloning.
 */
public class TestSetOfStrings {

	@Test
	public void testEquals() {
		SetOfStrings s1 = new SetOfStrings();
		s1.addString("A");
		s1.addString("B");
		SetOfStrings s2 = new SetOfStrings();
		s2.addString("A");
		s2.addString("B");

		assertEquals(s1, s2);
	}

	@Test
	public void testWriteAndRead() {
		try {
			SetOfStrings s1 = new SetOfStrings();
			s1.addString("A");
			s1.addString("B");
			SetOfStrings s2 = new SetOfStrings();
			s2.addString("C");
			s2.addString("D");

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			s1.write(out);
			s2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			SetOfStrings read1 = new SetOfStrings();
			read1.readFields(in);
			SetOfStrings read2 = new SetOfStrings();
			read2.readFields(in);

			assertEquals(s1, read1);
			assertEquals(s2, read2);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		SetOfStrings s1 = new SetOfStrings();
		s1.addString("A");
		s1.addString("B");
		SetOfStrings s2 = new SetOfStrings();
		s2.addString("C");
		s2.addString("D");

		// Merge and check merged result is correct
		s1.merge(s2);

		Set<String> expected = new HashSet<String>();
		expected.add("A");
		expected.add("B");
		expected.add("C");
		expected.add("D");
		SetOfStrings expectedResult = new SetOfStrings(expected);
		assertEquals(expectedResult, s1);
		
		// Check s2 hasn't changed
		expected.clear();
		expected.add("C");
		expected.add("D");
		expectedResult = new SetOfStrings(expected);
		assertEquals(expectedResult, s2);
	}

	@Test
	public void testConstructorClonesSet() {
		// Create SetOfStrings from Set
		Set<String> s1 = new HashSet<String>();
		s1.add("A");
		s1.add("B");
		SetOfStrings s = new SetOfStrings(s1);
		
		// Change original set
		s1.add("C");
		
		// Check SetOfStrings hasn't changed
		SetOfStrings s2 = new SetOfStrings();
		s2.addString("A");
		s2.addString("B");
		assertEquals(s2, s);
		
		// Add to SetOfStrings
		s1.remove("C");
		s.addString("X");
		
		// Check that Set hasn't changed
		Set<String> expected = new HashSet<String>();
		expected.add("A");
		expected.add("B");
		assertEquals(expected, s1);
	}
	
}
