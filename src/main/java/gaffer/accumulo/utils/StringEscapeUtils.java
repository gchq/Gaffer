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
package gaffer.accumulo.utils;

import gaffer.accumulo.Constants;

/**
 * Utility methods to escape strings so that several strings can be inserted into
 * Accumulo using the <code>Constants.DELIMITER</code> character to delimit them.
 */
public class StringEscapeUtils {

	// Note: We use Constants.DELIMITER_PLUS_ONE as the escape character. The
	// Constants.DELIMITER_PLUS_ONE character is used when forming a range to
	// search for all information relating to an entity - it is used as the end
	// of the range. For the purposes of mapping a range to a Bloom filter key
	// (see ElementFunctor) we need to know whether Constants.DELIMITER_PLUS_ONE
	// character is part of the actual value of the entity, or used as an
	// escape character. By using this character as the escape character we have
	// a character that is greater than the 0 byte, and we can tell whether it
	// was part of the original value.
	private static final char ESCAPE_CHAR = Constants.DELIMITER_PLUS_ONE;
	private static final char REPLACEMENT_CHAR = '\u0002';
	
	private StringEscapeUtils() {}
	
	/**
	 * Escapes the provided string so that it no longer contains the
	 * Constants.DELIMITER character.
	 * 
	 * @param s  The string to escape
	 * @return The escaped string
	 */
	public static String escape(String s) {
		StringBuilder sb = new StringBuilder(s.length());
		for (char c : s.toCharArray()) {
			if (c == ESCAPE_CHAR) {
				sb.append(ESCAPE_CHAR);
				sb.append(ESCAPE_CHAR);
			} else if (c == Constants.DELIMITER) {
				sb.append(ESCAPE_CHAR);
				sb.append(REPLACEMENT_CHAR);
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	/**
	 * Unescapes the provided string - this should only be called on
	 * strings that have been through the <code>escape</code> method.
	 * 
	 * @param s  The string to unescape
	 * @return The unescaped string
	 */
	public static String unEscape(String s) {
		StringBuilder sb = new StringBuilder(s.length());
		boolean isEscaped = false;
		for (char c : s.toCharArray()) {
			if (isEscaped) {
				if (c == ESCAPE_CHAR) {
					sb.append(ESCAPE_CHAR);
				} else if (c == REPLACEMENT_CHAR) {
					sb.append(Constants.DELIMITER);
				} else {
					sb.append(c);
				}
				isEscaped = false;
			} else {
				if (c == ESCAPE_CHAR) {
					isEscaped = true;
				} else {
					sb.append(c);
				}
			}
		}
		if (isEscaped) {
			throw new IllegalArgumentException("String was incorrectly escaped: " + s);
		}
		return sb.toString();
	}

}
