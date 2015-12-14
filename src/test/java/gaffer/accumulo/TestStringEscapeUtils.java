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
package gaffer.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import gaffer.accumulo.utils.StringEscapeUtils;

import org.junit.Test;

/**
 * Unit tests for the {@link StringEscapeUtils} class. Contains tests for
 * escaping and unescaping strings containing the delimiter character,
 * the escape character and the replacement character.
 */
public class TestStringEscapeUtils {

	// Note: these should match those in StringEscapeUtils in order for
	// some of the tests below to be meaningful. As these variables are
	// private in StringEscapeUtils, we need to repeat them here.
	private static final char ESCAPE_CHAR = '\u0001';
	private static final char REPLACEMENT_CHAR = '\u0002';
	
	@Test
	public void testNoDelims() {
		check("ssgsdgdfgwtwtgsg12341234");
	}

	@Test
	public void testWithOneDelim() {
		check("AAA" + Constants.DELIMITER + "AAA");
	}
	
	@Test
	public void testWithConsecutiveDelims() {
		check("AAA" + Constants.DELIMITER + Constants.DELIMITER + "AAA");
		check("AAA" + Constants.DELIMITER + Constants.DELIMITER + Constants.DELIMITER + "AAA");
	}

	@Test
	public void testWithEscapeCharacter() {
		check("AAA" + ESCAPE_CHAR + "AAA");
	}
	
	@Test
	public void testWithConsecutiveEscapeCharacter() {
		check("AAA" + ESCAPE_CHAR + ESCAPE_CHAR + "AAA");
		check("AAA" + ESCAPE_CHAR + ESCAPE_CHAR + ESCAPE_CHAR + "AAA");
	}
	
	@Test
	public void testWithReplacementCharacter() {
		check("AAA" + REPLACEMENT_CHAR + "AAA");
	}
	
	@Test
	public void testWithConsecutiveReplacementCharacter() {
		check("AAA" + REPLACEMENT_CHAR + REPLACEMENT_CHAR + "AAA");
		check("AAA" + REPLACEMENT_CHAR + REPLACEMENT_CHAR + REPLACEMENT_CHAR + "AAA");
	}
	
	@Test
	public void testWithEscapeThenReplacementCharacter() {
		check("AAA" + ESCAPE_CHAR + REPLACEMENT_CHAR + "AAA");
		check("AAA" + ESCAPE_CHAR + REPLACEMENT_CHAR + REPLACEMENT_CHAR + "AAA");
		check("AAA" + ESCAPE_CHAR + ESCAPE_CHAR + REPLACEMENT_CHAR + REPLACEMENT_CHAR + "AAA");
	}
	
	@Test
	public void testIllegalArgumentException() {
		// Create a string with an escape character at the end - this should
		// throw an IllegalArgumentException when we attempt to unescape it.
		try {
			StringEscapeUtils.unEscape("AAA" + ESCAPE_CHAR);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail("IllegalArgumentException not thrown");
	}
	
	/**
	 * Utility method to check that escaping the supplied string and then unescaping
	 * that results in the original string. Also checks that the escaped string
	 * doesn't contain the DELIMITER character.
	 * 
	 * @param string
	 */
	private static void check(String string) {
		String escapedString = StringEscapeUtils.escape(string);
		String unescapedEscapedString = StringEscapeUtils.unEscape(escapedString);
		assertEquals(string, unescapedEscapedString);
		assertFalse(escapedString.contains("" + Constants.DELIMITER));
	}

}
