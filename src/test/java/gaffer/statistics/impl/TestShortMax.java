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
 * Unit tests for {@link ShortMax}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestShortMax {

	@Test
	public void testWriteAndRead() {
		try {
			ShortMax sm1 = new ShortMax();
			sm1.setMax((short) 150);
			ShortMax sm2 = new ShortMax();
			sm2.setMax((short) 12);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			sm1.write(out);
			sm2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			ShortMax sm3 = new ShortMax();
			sm3.readFields(in);
			ShortMax sm4 = new ShortMax();
			sm4.readFields(in);
			assertEquals(sm1, sm3);
			assertEquals(sm2, sm4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		ShortMax sm1 = new ShortMax();
		sm1.setMax((short) 15);
		ShortMax sm2 = new ShortMax();
		sm2.setMax((short) 12);
		sm1.merge(sm2);
		assertEquals(15, sm1.getMax());
	}

}
