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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import org.junit.Test;

/**
 * Unit tests for {@link RegularExpressionPredicate}.
 */
public class TestRegularExpressionPredicate {

	@Test
	public void test() {
		Pattern summaryTypePattern = Pattern.compile("A.");
		Pattern summarySubTypePattern = Pattern.compile("^B");
		RegularExpressionPredicate predicate = new RegularExpressionPredicate(summaryTypePattern, summarySubTypePattern);
		assertTrue(predicate.accept("A123", "B123"));
		assertFalse(predicate.accept("A1", "aB"));
		assertFalse(predicate.accept("X", "YS"));
	}
	
	@Test
	public void testAnySubType() {
		Pattern summaryTypePattern = Pattern.compile("A.");
		Pattern summarySubTypePattern = Pattern.compile("");
		RegularExpressionPredicate predicate = new RegularExpressionPredicate(summaryTypePattern, summarySubTypePattern);
		assertTrue(predicate.accept("A123", "XYZ"));
		assertTrue(predicate.accept("A1", ""));
		assertFalse(predicate.accept("X", "YS"));
	}
	
	@Test
	public void testEquals() {
		Pattern summaryTypePattern1 = Pattern.compile("A.");
		Pattern summarySubTypePattern1 = Pattern.compile("^B");
		RegularExpressionPredicate predicate1 = new RegularExpressionPredicate(summaryTypePattern1, summarySubTypePattern1);
		Pattern summaryTypePattern2 = Pattern.compile("A.");
		Pattern summarySubTypePattern2 = Pattern.compile("^B");
		RegularExpressionPredicate predicate2 = new RegularExpressionPredicate(summaryTypePattern2, summarySubTypePattern2);
		assertEquals(predicate1, predicate2);
		summarySubTypePattern2 = Pattern.compile("^B*");
		predicate2 = new RegularExpressionPredicate(summaryTypePattern2, summarySubTypePattern2);
		assertNotEquals(predicate1, predicate2);
	}
	
	@Test
	public void testWriteRead() throws IOException {
		Pattern summaryTypePattern = Pattern.compile("A.");
		Pattern summarySubTypePattern = Pattern.compile("^B");
		RegularExpressionPredicate predicate = new RegularExpressionPredicate(summaryTypePattern, summarySubTypePattern);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		predicate.write(out);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream in = new DataInputStream(bais);
		RegularExpressionPredicate read = new RegularExpressionPredicate();
		read.readFields(in);
		assertEquals(predicate, read);
	}

}
