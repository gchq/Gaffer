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
package gaffer.predicate.summarytype.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

/**
 * Unit tests for {@link SummaryTypeInSetPredicate}.
 */
public class TestSummaryTypeInSetPredicate {

	@Test
	public void test() {
		SummaryTypeInSetPredicate predicate = new SummaryTypeInSetPredicate();
		predicate.addAllowedSummaryTypes("A", "B", "C");
		assertTrue(predicate.accept("A", "B"));
		assertTrue(predicate.accept("B", "X"));
		assertTrue(predicate.accept("C", "Y"));
		assertFalse(predicate.accept("D", "D"));
	}

	@Test
	public void testWriteRead() throws IOException {
		SummaryTypeInSetPredicate predicate = new SummaryTypeInSetPredicate();
		predicate.addAllowedSummaryTypes("A", "B", "C");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		predicate.write(out);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream in = new DataInputStream(bais);
		SummaryTypeInSetPredicate read = new SummaryTypeInSetPredicate();
		read.readFields(in);
		assertEquals(predicate, read);
	}
	
}
