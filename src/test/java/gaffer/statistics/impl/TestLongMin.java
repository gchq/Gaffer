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
 * Unit tests for {@link LongMin}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestLongMin {

	@Test
	public void testWriteAndRead() {
		try {
			LongMin lm1 = new LongMin();
			lm1.setMin(150L);
			LongMin lm2 = new LongMin();
			lm2.setMin(12L);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			lm1.write(out);
			lm2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			LongMin lm3 = new LongMin();
			lm3.readFields(in);
			LongMin lm4 = new LongMin();
			lm4.readFields(in);
			assertEquals(lm1, lm3);
			assertEquals(lm2, lm4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		LongMin lm1 = new LongMin();
		lm1.setMin(15000L);
		LongMin lm2 = new LongMin();
		lm2.setMin(12000L);
		lm1.merge(lm2);
		assertEquals(12000L, lm1.getMin());
	}

}
