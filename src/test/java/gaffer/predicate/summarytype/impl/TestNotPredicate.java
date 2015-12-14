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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * Unit tests for {@link NotPredicate}.
 */
public class TestNotPredicate {

	@Test
	public void testNot() {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(types);
		// NOT
		NotPredicate notPredicate = new NotPredicate(predicate1);
		assertTrue(notPredicate.accept("BBB", "CCC"));
		assertFalse(notPredicate.accept("AAA", "CCC"));
	}
	
	@Test
	public void testEquals() {
		Set<String> types = new HashSet<String>();
		types.add("AAA");
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(types);
		SummaryTypePredicate predicate2 = new SummaryTypeInSetPredicate(types);
		NotPredicate notPredicate1 = new NotPredicate(predicate1);
		NotPredicate notPredicate2 = new NotPredicate(predicate2);
		assertEquals(notPredicate1, notPredicate2);
		Set<String> types2 = new HashSet<String>(types);
		types2.add("BBB");
		predicate2 = new SummaryTypeInSetPredicate(types2);
		notPredicate2 = new NotPredicate(predicate2);
		assertNotEquals(notPredicate1, notPredicate2);
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
		NotPredicate notPredicate1 = new NotPredicate(predicate1);
		NotPredicate notPredicate2 = new NotPredicate(predicate2);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		notPredicate1.write(out);
		notPredicate2.write(out);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream in = new DataInputStream(bais);
		NotPredicate read1 = new NotPredicate();
		NotPredicate read2 = new NotPredicate();
		read1.readFields(in);
		read2.readFields(in);
		assertEquals(notPredicate1, read1);
		assertEquals(notPredicate2, read2);
	}

}
