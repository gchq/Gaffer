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

import org.junit.Test;

/**
 * Unit tests for {@link IntMax}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestIntMax {

	@Test
	public void testWriteAndRead() {
		try {
			IntMax im1 = new IntMax();
			im1.setMax(150000);
			IntMax im2 = new IntMax();
			im2.setMax(12);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			im1.write(out);
			im2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			IntMax im3 = new IntMax();
			im3.readFields(in);
			IntMax im4 = new IntMax();
			im4.readFields(in);
			assertEquals(im1, im3);
			assertEquals(im2, im4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		IntMax im1 = new IntMax();
		im1.setMax(15);
		IntMax im2 = new IntMax();
		im2.setMax(12);
		im1.merge(im2);
		assertEquals(15, im1.getMax());
	}

}
