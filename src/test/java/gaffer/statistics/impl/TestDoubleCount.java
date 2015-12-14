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
 * Unit tests for {@link DoubleCount}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestDoubleCount {

	@Test
	public void testWriteAndRead() {
		try {
			DoubleCount count1 = new DoubleCount();
			count1.setCount(150000.0);
			DoubleCount count2 = new DoubleCount();
			count2.setCount(12.0);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			count1.write(out);
			count2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			DoubleCount count3 = new DoubleCount();
			count3.readFields(in);
			DoubleCount count4 = new DoubleCount();
			count4.readFields(in);
			assertEquals(count1, count3);
			assertEquals(count2, count4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		DoubleCount count1 = new DoubleCount();
		count1.setCount(15.0);
		DoubleCount count2 = new DoubleCount();
		count2.setCount(12.0);
		count1.merge(count2);
		assertEquals(27.0D, count1.getCount(), 0.0D);
	}

}
