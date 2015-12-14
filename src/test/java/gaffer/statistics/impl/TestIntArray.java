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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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

import org.junit.Test;

/**
 * Basic tests for {@link IntArray}. The <code>write()</code>, <code>read()</code> and <code>merge()</code>
 * methods are tested.
 */
public class TestIntArray {

	@Test
	public void testWriteAndRead() {
		try {
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			IntArray ia2 = new IntArray(new int[]{5,6,7,8});

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			ia1.write(out);
			ia2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			IntArray ia3 = new IntArray();
			ia3.readFields(in);
			IntArray ia4 = new IntArray();
			ia4.readFields(in);
			assertEquals(ia1, ia3);
			assertEquals(ia2, ia4);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		IntArray ia1 = new IntArray(new int[]{1,2,3,4});
		IntArray ia2 = new IntArray(new int[]{5,6,7,8});
		ia1.merge(ia2);
		assertArrayEquals(ia1.getArray(), new int[]{6,8,10,12});
		assertArrayEquals(ia2.getArray(), new int[]{5,6,7,8});
	}

	@Test
	public void testFailedMerge() {
		try {
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			IntArray ia2 = new IntArray(new int[]{5,6,7});
			ia1.merge(ia2);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testWriteAndReadWhenWrittenAsDense() {
		try {
			IntArray ia = new IntArray(new int[]{1,2,3,4,5,6,7,8});

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			ia.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			IntArray ia2 = new IntArray();
			ia2.readFields(in);
			assertEquals(ia, ia2);
		} catch (IOException e) {
			fail("Exception in testSparse() " + e);
		}
	}

	@Test
	public void testWriteAndReadWhenWrittenAsSparse() {
		try {
			IntArray ia = new IntArray(new int[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1});

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			ia.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			IntArray ia2 = new IntArray();
			ia2.readFields(in);
			assertEquals(ia, ia2);
		} catch (IOException e) {
			fail("Exception in testSparse() " + e);
		}
	}

	@Test
	public void testWriteReadWhenAllZero() {
		try {
			IntArray ia = new IntArray(new int[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			ia.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			IntArray ia2 = new IntArray();
			ia2.readFields(in);
			assertEquals(ia, ia2);
		} catch (IOException e) {
			fail("Exception in testSparse() " + e);
		}
	}
	
	@Test
	public void testSparseIsMoreCompact() {
		try {
			IntArray sparse = new IntArray(new int[]{0,0,0,0,0,0,0,0,0,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,1});
			IntArray dense  = new IntArray(new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24});

			ByteArrayOutputStream baosSparse = new ByteArrayOutputStream();
			DataOutput outSparse = new DataOutputStream(baosSparse);
			sparse.write(outSparse);

			ByteArrayOutputStream baosDense = new ByteArrayOutputStream();
			DataOutput outDense = new DataOutputStream(baosDense);
			dense.write(outDense);
			
			assertTrue(baosSparse.toByteArray().length < baosDense.toByteArray().length);
		} catch (IOException e) {
			fail("Exception in testSparseIsMoreCompact() " + e);
		}
	}
	
	@Test
	public void testClone() {
		// Create an IntArray
		IntArray ia = new IntArray(new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24});
		
		// Clone it
		IntArray clonedIa = ia.clone();

		// Check clone is the same as the original
		assertEquals(ia, clonedIa);
		
		// Change original
		ia.setArray(new int[]{0,0,0,0,0,0,0,0,1});
		
		// Check clone is not equal to the orginal
		assertNotEquals(ia, clonedIa);
	}
	
}
