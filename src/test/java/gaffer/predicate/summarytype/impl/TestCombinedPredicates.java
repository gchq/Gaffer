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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import gaffer.Pair;
import gaffer.predicate.summarytype.SummaryTypePredicate;
import gaffer.predicate.summarytype.impl.CombinedPredicates.Combine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Unit tests for {@link CombinedPredicates}.
 */
public class TestCombinedPredicates {

	@Test
	public void testCombinesCorrectly() {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		types.add("GGG");
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(types);
		Set<Pair<String>> pairs = new HashSet<Pair<String>>();
		pairs.add(new Pair<String>("AAA", "BBB"));
		pairs.add(new Pair<String>("DDD", "FFF"));
		SummaryTypePredicate predicate2 = new SummaryTypeAndSubTypeInSetPredicate(pairs);
		// AND
		CombinedPredicates combined = new CombinedPredicates(predicate1, predicate2, Combine.AND);
		assertTrue(combined.accept("AAA", "BBB"));
		assertFalse(combined.accept("AAA", "CCC"));
		assertFalse(combined.accept("BBB", "CCC"));
		// OR
		combined = new CombinedPredicates(predicate1, predicate2, Combine.OR);
		assertTrue(combined.accept("AAA", "BBB"));
		assertTrue(combined.accept("AAA", "CCC"));
		assertFalse(combined.accept("BBB", "CCC"));
		assertTrue(combined.accept("DDD", "FFF"));
		// XOR
		combined = new CombinedPredicates(predicate1, predicate2, Combine.XOR);
		assertTrue(combined.accept("GGG", "BBB"));
		assertTrue(combined.accept("DDD", "FFF"));
		assertFalse(combined.accept("AAA", "BBB"));
		assertFalse(combined.accept("XXX", "XXX"));
	}
	
	@Test
	public void test() {
		Set<String> types = new HashSet<String>();
		types.add("A");
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(types);
		Set<Pair<String>> pairs = new HashSet<Pair<String>>();
		pairs.add(new Pair<String>("A", "B"));
		pairs.add(new Pair<String>("DDD", "FFF"));
		SummaryTypePredicate predicate2 = new SummaryTypeAndSubTypeInSetPredicate(pairs);
		SummaryTypePredicate predicate3 = new NotPredicate(predicate2);
		CombinedPredicates combined = new CombinedPredicates(predicate1, predicate3, Combine.AND);
		assertTrue(combined.accept("A", "C"));
		assertFalse(combined.accept("A", "B"));
		assertFalse(combined.accept("B", "B"));
	}
	
	@Test
	public void testEquals() {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		types.add("GGG");
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(types);
		Set<Pair<String>> pairs = new HashSet<Pair<String>>();
		pairs.add(new Pair<String>("AAA", "BBB"));
		pairs.add(new Pair<String>("DDD", "FFF"));
		SummaryTypePredicate predicate2 = new SummaryTypeAndSubTypeInSetPredicate(pairs);
		CombinedPredicates combined1 = new CombinedPredicates(predicate1, predicate2, Combine.AND);
		CombinedPredicates combined2 = new CombinedPredicates(predicate1, predicate2, Combine.AND);
		assertEquals(combined1, combined2);
		combined2 = new CombinedPredicates(predicate1, predicate2, Combine.OR);
		assertNotEquals(combined1, combined2);
		combined2 = new CombinedPredicates(predicate1, predicate2, Combine.XOR);
		assertNotEquals(combined1, combined2);
		combined2 = new CombinedPredicates(predicate1, predicate1, Combine.AND);
		assertNotEquals(combined1, combined2);
	}
	
	@Test
	public void testWriteRead() throws IOException {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		types.add("GGG");
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(types);
		Set<Pair<String>> pairs = new HashSet<Pair<String>>();
		pairs.add(new Pair<String>("AAA", "BBB"));
		pairs.add(new Pair<String>("DDD", "FFF"));
		SummaryTypePredicate predicate2 = new SummaryTypeAndSubTypeInSetPredicate(pairs);
		// AND
		CombinedPredicates combined = new CombinedPredicates(predicate1, predicate2, Combine.AND);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		combined.write(out);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream in = new DataInputStream(bais);
		CombinedPredicates read = new CombinedPredicates();
		read.readFields(in);
		assertEquals(combined, read);
		// OR
		combined = new CombinedPredicates(predicate1, predicate2, Combine.AND);
		baos = new ByteArrayOutputStream();
		out = new DataOutputStream(baos);
		combined.write(out);
		bais = new ByteArrayInputStream(baos.toByteArray());
		in = new DataInputStream(bais);
		read = new CombinedPredicates();
		read.readFields(in);
		assertEquals(combined, read);
		// XOR
		combined = new CombinedPredicates(predicate1, predicate2, Combine.XOR);
		baos = new ByteArrayOutputStream();
		out = new DataOutputStream(baos);
		combined.write(out);
		bais = new ByteArrayInputStream(baos.toByteArray());
		in = new DataInputStream(bais);
		read = new CombinedPredicates();
		read.readFields(in);
		assertEquals(combined, read);
	}

}
