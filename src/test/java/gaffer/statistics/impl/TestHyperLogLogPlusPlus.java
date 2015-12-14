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

import org.junit.Test;

/**
 * Unit tests for {@link HyperLogLogPlusPlus}. Contains tests for writing and reading,
 * for merging, for equals, for hash, etc.
 */
public class TestHyperLogLogPlusPlus {

	@Test
	public void testWriteAndRead() {
		try {
			HyperLogLogPlusPlus hllpp1 = new HyperLogLogPlusPlus();
			hllpp1.add("A");
			hllpp1.add("B");
			HyperLogLogPlusPlus hllpp2 = new HyperLogLogPlusPlus();
			hllpp2.add("A");
			hllpp2.add("B");

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			hllpp1.write(out);
			hllpp2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			HyperLogLogPlusPlus hllpp3 = new HyperLogLogPlusPlus();
			hllpp3.readFields(in);
			HyperLogLogPlusPlus hllpp4 = new HyperLogLogPlusPlus();
			hllpp4.readFields(in);

			assertEquals(hllpp1, hllpp3);
			assertEquals(hllpp2, hllpp4);
			assertEquals(hllpp1.getCardinalityEstimate(), hllpp3.getCardinalityEstimate());
			assertEquals(hllpp2.getCardinalityEstimate(), hllpp4.getCardinalityEstimate());
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testWriteAndReadWhenEmpty() {
		try {
			HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus();

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			hllpp.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			HyperLogLogPlusPlus hllpp2 = new HyperLogLogPlusPlus();
			hllpp2.readFields(in);

			assertEquals(hllpp.getCardinalityEstimate(), hllpp2.getCardinalityEstimate());
		} catch (IOException e) {
			fail("Exception in testWriteAndReadWhenEmpty() " + e);
		}
	}

	@Test
	public void testMerge() {
		HyperLogLogPlusPlus hllpp1 = new HyperLogLogPlusPlus();
		hllpp1.add("A");
		hllpp1.add("B");
		HyperLogLogPlusPlus hllpp2 = new HyperLogLogPlusPlus();
		hllpp2.add("C");
		hllpp2.add("D");
		assertEquals(2, hllpp1.getCardinalityEstimate()); // This is a sketch, so no guarantee of a correct count but in practice
		// it will be correct for low cardinalities.
		hllpp1.merge(hllpp2);
		assertEquals(4, hllpp1.getCardinalityEstimate()); // Again, no guarantee of a correct count but in practice
		// it will be correct for low cardinalities, and this lets us test that data has been merged in.
	}

	@Test
	public void testFailedMerge() {
		try {
			HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus();
			hllpp.add("A");
			hllpp.add("B");
			IntArray ia = new IntArray(new int[]{5,6,7});
			hllpp.merge(ia);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testClone() {
		// Create a HyperLogLogPlusPlus sketch
		HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus();
		hllpp.add("A");
		hllpp.add("B");

		// Clone it
		HyperLogLogPlusPlus clonedHllpp = hllpp.clone();

		// Check clone is the same as the original
		assertEquals(hllpp, clonedHllpp);
		assertEquals(hllpp.getCardinalityEstimate(), clonedHllpp.getCardinalityEstimate());

		// Change original
		long originalEstimate = hllpp.getCardinalityEstimate();
		for (int i = 0; i < 100; i++) {
			hllpp.add("" + i);
		}

		// Check clone is not equal to the original
		assertNotEquals(hllpp.getCardinalityEstimate(), clonedHllpp.getCardinalityEstimate());

		// Check estimate from clone has not changed
		assertEquals(originalEstimate, clonedHllpp.getCardinalityEstimate());
	}

	@Test
	public void testCloneWhenEmpty() {
		HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus();
		HyperLogLogPlusPlus clone = hllpp.clone();

		assertEquals(hllpp.getCardinalityEstimate(), clone.getCardinalityEstimate());
	}

	@Test
	public void testCloneOfBusySketch() {
		HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus();
		for (int i = 0; i < 10000; i++) {
			hllpp.add("" + i);
		}
		HyperLogLogPlusPlus clone = hllpp.clone();
		assertEquals(hllpp.getCardinalityEstimate(), clone.getCardinalityEstimate());
	}

	@Test
	public void testEqualsAndHashCode() {
		HyperLogLogPlusPlus hllpp1 = new HyperLogLogPlusPlus();
		hllpp1.add("A");
		hllpp1.add("B");
		hllpp1.add("C");
		hllpp1.add("D");
		HyperLogLogPlusPlus hllpp2 = new HyperLogLogPlusPlus();
		hllpp2.add("B");
		hllpp2.add("A");
		hllpp2.add("D");
		hllpp2.add("C");
		assertEquals(hllpp1, hllpp2);
		assertEquals(hllpp1.hashCode(), hllpp2.hashCode());
		hllpp1.add("E");
		hllpp1.add("F");
		assertNotEquals(hllpp1, hllpp2);
		assertNotEquals(hllpp1.hashCode(), hllpp2.hashCode());

		for (int n : new int[]{10, 100, 1000, 10000}) {
			String[] strings = new String[n];
			for (int i = 0; i < n; i++) {
				strings[i] = createRandomString();
			}
			hllpp1 = new HyperLogLogPlusPlus();
			hllpp2 = new HyperLogLogPlusPlus();
			for (int i = 0; i < n; i++) {
				hllpp1.add(strings[i]);
				hllpp2.add(strings[n - 1 - i]);
			}
			assertEquals(hllpp1, hllpp2);
			assertEquals(hllpp1.hashCode(), hllpp2.hashCode());
		}
	}

	private static String createRandomString() {
		char[] chars = new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
		String s = "";
		for (int i = 0; i < 10; i++) {
			s += chars[(int) (Math.random() * chars.length)];
		}
		return s;
	}

}
