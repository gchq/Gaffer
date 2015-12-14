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
 * Basic tests for {@link MaxDouble}. The <code>write()</code>, <code>read()</code>
 * and <code>merge()</code> methods are tested.
 */
public class TestMaxDouble {

	@Test
	public void testEquals() {
		MaxDouble md1 = new MaxDouble(5d / 2);
		MaxDouble md2 = new MaxDouble();
		MaxDouble md3 = new MaxDouble(7.8d);
		md2.setMax(2.5d);

		assertEquals(md1, md2);
		assertEquals(md1.getMax(), md2.getMax(), 0.0d);
		assertNotEquals(md1, md3);
		assertNotEquals(md2, md3);
	}

	@Test
	public void testWriteAndRead() {
		try {
			MaxDouble md1 = new MaxDouble(5d / 2);
			MaxDouble md2 = new MaxDouble();
			md2.setMax(7.8d);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			md1.write(out);
			md2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(
					baos.toByteArray()));
			MaxDouble md3 = new MaxDouble();
			md3.readFields(in);
			MaxDouble md4 = new MaxDouble();
			md4.readFields(in);
			assertEquals(md1, md3);
			assertEquals(md2, md4);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		MaxDouble md1 = new MaxDouble(5d / 2);
		MaxDouble md2 = new MaxDouble(3.3d);
		MaxDouble md3 = new MaxDouble(7.8d);

		MaxDouble md4 = new MaxDouble(3.3d);
		MaxDouble md5 = new MaxDouble(7.8d);

		md1.merge(md2);
		md3.merge(md2);

		assertEquals(md1, md4);
		assertEquals(md3, md5);
	}

	@Test
	public void testFailedMerge() {
		MaxDouble md = new MaxDouble(42.0d);
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
