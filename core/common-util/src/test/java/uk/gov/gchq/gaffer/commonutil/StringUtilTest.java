/*
 * Copyright 2018-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.commonutil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StringUtilTest {

    @Test
    void unescapeCommaShouldReplaceBackslashesWithComma() {
        assertEquals("replaceBackslashesWith,Comma", StringUtil.unescapeComma("replaceBackslashesWith\\\\Comma"));
    }

    @Test
    void unescapeCommaShouldNotRemoveSemicolon() {
        assertEquals("don'tRemove;SemiColon", StringUtil.unescapeComma("don'tRemove;SemiColon"));
    }

    @Test
    void unescapeCommaShouldNotRemoveComma() {
        assertEquals("don'tRemove,Comma", StringUtil.unescapeComma("don'tRemove,Comma"));
    }

    @Test
    void unescapeCommaShouldRemoveBackslash() {
        assertEquals("removeBackslash", StringUtil.unescapeComma("remove\\Backslash"));
    }

    @Test
    void escapeCommaShouldReplaceBackslashWithSemiColon() {
        assertEquals("replaceWith\\;semicolon", StringUtil.escapeComma("replaceWith\\semicolon"));
    }

    @Test
    void escapeCommaShouldReplaceCommaWith2Backslashes() {
        assertEquals("replaceWith\\\\comma", StringUtil.escapeComma("replaceWith,comma"));
    }

    @Test
    void toBytesWhenStringIsValid() {
        assertArrayEquals("isValid".getBytes(), StringUtil.toBytes("isValid"));
    }

    @Test
    void toBytesWhenStringIsNull() {
        assertArrayEquals(new byte[0], StringUtil.toBytes(null));
    }

    @Test
    void toStringWhenBytesAreValid() {
        final byte[] validBytes = "isValid".getBytes();

        assertEquals("isValid", StringUtil.toString(validBytes));
    }

    @Test
    void ifEmptyStringTestReturnNull() {
        assertNull(StringUtil.nullIfEmpty(""));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {" ", "String", " string "})
    void shouldReturnValueWhenNotEmptyString(String input) {
        assertEquals(input, StringUtil.nullIfEmpty(input));
    }
}
