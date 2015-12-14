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
 * Basic tests for {@link MinDouble}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMinDouble {

	@Test
	public void testEquals() {
		MinDouble md1 = new MinDouble(5d / 2);
		MinDouble md2 = new MinDouble();
		MinDouble md3 = new MinDouble(7.8d);
		md2.setMin(2.5d);

		assertEquals(md1, md2);
		assertEquals(md1.getMin(), md2.getMin(), 0.0d);
		assertNotEquals(md1, md3);
		assertNotEquals(md2, md3);
	}

	@Test
	public void testWriteAndRead() {
		try {
			MinDouble md1 = new MinDouble(5d / 2);
			MinDouble md2 = new MinDouble();
			md2.setMin(7.8d);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			md1.write(out);
			md2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(
					baos.toByteArray()));
			MinDouble md3 = new MinDouble();
			md3.readFields(in);
			MinDouble md4 = new MinDouble();
			md4.readFields(in);
			assertEquals(md1, md3);
			assertEquals(md2, md4);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		MinDouble md1 = new MinDouble(5d / 2);
		MinDouble md2 = new MinDouble(3.3d);
		MinDouble md3 = new MinDouble(7.8d);

		MinDouble md4 = new MinDouble(2.5d);
		MinDouble md5 = new MinDouble(3.3d);

		md1.merge(md2);
		md3.merge(md2);

		assertEquals(md1, md4);
		assertEquals(md3, md5);
	}

	@Test
	public void testFailedMerge() {
		MinDouble md = new MinDouble(42.0d);
		LongCount lc = new LongCount(42);

		try {
			md.merge(lc);
		} catch (IllegalArgumentException e) {
			return;
		}

		fail("Allowed merge between Statistics of differing types. "
				+ "IllegalArgumentException should have been thrown.");
	}
}
