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
package gaffer.predicate.typevalue.impl;

import gaffer.predicate.typevalue.TypeValuePredicate;
import org.junit.Test;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link CombinedPredicates}.
 */
public class TestCombinedPredicates {

	@Test
	public void testCombinesCorrectly() {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		types.add("GGG");
		TypeValuePredicate predicate1 = new TypeInSetPredicate(types);
		Set<String> values = new HashSet<String>();
		values.add("LLL");
		TypeValuePredicate predicate2 = new ValueInSetPredicate(values);
		// AND
		CombinedPredicates combined = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.AND);
		assertTrue(combined.accept("AAA", "LLL"));
		assertFalse(combined.accept("AAA", "CCC"));
		assertFalse(combined.accept("BBB", "CCC"));
		// OR
		combined = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.OR);
		assertTrue(combined.accept("AAA", "BBB"));
		assertTrue(combined.accept("GGG", "CCC"));
		assertFalse(combined.accept("BBB", "CCC"));
		assertTrue(combined.accept("DDD", "LLL"));
		// XOR
		combined = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.XOR);
		assertTrue(combined.accept("GGG", "BBB"));
		assertTrue(combined.accept("DDD", "LLL"));
		assertFalse(combined.accept("AAA", "LLL"));
		assertFalse(combined.accept("XXX", "XXX"));
	}

	@Test
	public void testEquals() {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		types.add("GGG");
		TypeValuePredicate predicate1 = new TypeInSetPredicate(types);
		Set<String> values = new HashSet<String>();
		values.add("LLL");
		TypeValuePredicate predicate2 = new ValueInSetPredicate(values);
		CombinedPredicates combined1 = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.AND);
		CombinedPredicates combined2 = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.AND);
		assertEquals(combined1, combined2);
		combined2 = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.OR);
		assertNotEquals(combined1, combined2);
		combined2 = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.XOR);
		assertNotEquals(combined1, combined2);
		combined2 = new CombinedPredicates(predicate1, predicate1, CombinedPredicates.Combine.AND);
		assertNotEquals(combined1, combined2);
	}

	@Test
	public void testWriteRead() throws IOException {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		types.add("GGG");
		TypeValuePredicate predicate1 = new TypeInSetPredicate(types);
		Set<String> values = new HashSet<String>();
		values.add("LLL");
		TypeValuePredicate predicate2 = new ValueInSetPredicate(values);
		// AND
		CombinedPredicates combined = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.AND);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		combined.write(out);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream in = new DataInputStream(bais);
		CombinedPredicates read = new CombinedPredicates();
		read.readFields(in);
		assertEquals(combined, read);
		// OR
		combined = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.AND);
		baos = new ByteArrayOutputStream();
		out = new DataOutputStream(baos);
		combined.write(out);
		bais = new ByteArrayInputStream(baos.toByteArray());
		in = new DataInputStream(bais);
		read = new CombinedPredicates();
		read.readFields(in);
		assertEquals(combined, read);
		// XOR
		combined = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.XOR);
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
