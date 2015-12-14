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
 * Unit tests for {@link DoubleProductViaLogs}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestDoubleProductViaLogs {

	private static final double DELTA = 1e-10;
	
	@Test
	public void testWriteAndRead() {
		try {
			DoubleProductViaLogs product1 = new DoubleProductViaLogs();
			product1.setProduct(10.0D);
			DoubleProductViaLogs product2 = new DoubleProductViaLogs();
			product2.setProduct(12.0D);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			product1.write(out);
			product2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			DoubleProductViaLogs product3 = new DoubleProductViaLogs();
			product3.readFields(in);
			DoubleProductViaLogs product4 = new DoubleProductViaLogs();
			product4.readFields(in);
			assertEquals(product1, product3);
			assertEquals(product2, product4);
			
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		DoubleProductViaLogs product1 = new DoubleProductViaLogs();
		product1.setProduct(10.0D);
		DoubleProductViaLogs product2 = new DoubleProductViaLogs();
		product2.setProduct(12.0D);
		product1.merge(product2);
		assertEquals(10.0D * 12.0D, product1.getProduct(), DELTA);
	}

	@Test
	public void testLog() {
		DoubleProductViaLogs product = new DoubleProductViaLogs();
		product.setProduct(12.0D);
		assertEquals(12.0D, product.getProduct(), DELTA);
	}
	
}
