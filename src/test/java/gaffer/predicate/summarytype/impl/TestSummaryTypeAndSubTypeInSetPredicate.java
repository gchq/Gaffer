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
import gaffer.Pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Unit tests for {@link SummaryTypeAndSubTypeInSetPredicate}.
 */
public class TestSummaryTypeAndSubTypeInSetPredicate {

	@Test
	public void test() {
		SummaryTypeAndSubTypeInSetPredicate predicate = new SummaryTypeAndSubTypeInSetPredicate();
		Set<Pair<String>> pairs = new HashSet<Pair<String>>();
		pairs.add(new Pair<String>("A", "B"));
		pairs.add(new Pair<String>("B", "C"));
		pairs.add(new Pair<String>("D", "E"));
		predicate.addAllowedSummaryTypes(pairs);
		assertTrue(predicate.accept("A", "B"));
		assertTrue(predicate.accept("B", "C"));
		assertTrue(predicate.accept("D", "E"));
		assertFalse(predicate.accept("B", "B"));
		assertFalse(predicate.accept("D", "D"));
	}

	@Test
	public void testWriteRead() throws IOException {
		SummaryTypeAndSubTypeInSetPredicate predicate = new SummaryTypeAndSubTypeInSetPredicate();
		Set<Pair<String>> pairs = new HashSet<Pair<String>>();
		pairs.add(new Pair<String>("A", "B"));
		pairs.add(new Pair<String>("B", "C"));
		pairs.add(new Pair<String>("D", "E"));
		predicate.addAllowedSummaryTypes(pairs);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		predicate.write(out);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream in = new DataInputStream(bais);
		SummaryTypeAndSubTypeInSetPredicate read = new SummaryTypeAndSubTypeInSetPredicate();
		read.readFields(in);
		assertEquals(predicate, read);
	}
	
}
