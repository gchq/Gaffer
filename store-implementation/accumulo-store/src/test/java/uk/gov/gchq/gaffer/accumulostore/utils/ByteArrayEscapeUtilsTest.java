/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ByteArrayEscapeUtilsTest {

    private static final byte ESCAPE_CHAR = (byte) 1;
    private static final byte REPLACEMENT_CHAR = (byte) 2;

    @Test
    public void testNoDelims() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30};
        check(bytes);
    }

    @Test
    public void testWithOneDelim() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ByteArrayEscapeUtils.DELIMITER, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithConsecutiveDelims() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER, (byte) 40, (byte) 50};
        check(bytes);
        final byte[] updatedBytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER, (byte) 40, (byte) 50};
        check(updatedBytes);
    }

    @Test
    public void testWithEscapeCharacter() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithConsecutiveEscapeCharacter() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, ESCAPE_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        final byte[] updatedBytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, ESCAPE_CHAR, ESCAPE_CHAR, (byte) 40, (byte) 50};
        check(updatedBytes);
    }

    @Test
    public void testWithReplacementCharacter() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithConsecutiveReplacementCharacter() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        final byte[] updatedBytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, REPLACEMENT_CHAR, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(updatedBytes);
    }

    @Test
    public void testWithEscapeThenReplacementCharacter() {
        final byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        final byte[] bytesTwoReplacementChar = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytesTwoReplacementChar);
        final byte[] twoEscapeAndReplacementCharbytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, ESCAPE_CHAR, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(twoEscapeAndReplacementCharbytes);
    }

    @Test
    public void testRandom() {
        for (int i = 0; i < 100000; i++) {
            final int length = RandomUtils.nextInt(0, 100) + 1;
            final byte[] b = new byte[length];
            for (int j = 0; j < b.length; j++) {
                b[j] = (byte) RandomUtils.nextInt(0, Integer.MAX_VALUE);
            }
            check(b);
        }
    }

    @Test
    public void testOrdering() {
        // Generate some keys with row key formed from random bytes, and add to ordered set
        final SortedSet<Key> original = new TreeSet<>();
        for (int i = 0; i < 100000; i++) {
            final int length = RandomUtils.nextInt(0, 100) + 1;
            final byte[] b = new byte[length];
            for (int j = 0; j < b.length; j++) {
                b[j] = (byte) RandomUtils.nextInt(0, Integer.MAX_VALUE);
            }
            final Key key = new Key(b, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
            original.add(key);
        }
        // Loop through set, check that ordering is preserved after escaping
        final Iterator<Key> it = original.iterator();
        Key first = it.next();
        Key second = it.next();
        while (true) {
            assertTrue(first.compareTo(second) < 0);
            final Key escapedFirst = new Key(ByteArrayEscapeUtils.escape(first.getRowData().getBackingArray()), AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
            final Key escapedSecond = new Key(ByteArrayEscapeUtils.escape(second.getRowData().getBackingArray()), AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
            assertTrue(escapedFirst.compareTo(escapedSecond) < 0);
            first = second;
            if (it.hasNext()) {
                second = it.next();
            } else {
                break;
            }
        }
    }

    private void check(final byte[] bytes) {
        byte[] escaped = ByteArrayEscapeUtils.escape(bytes);
        byte[] unescaped = ByteArrayEscapeUtils.unEscape(escaped);
        assertArrayEquals(bytes, unescaped);
        for (final byte anEscaped : escaped) {
            assertNotEquals(ByteArrayEscapeUtils.DELIMITER, anEscaped);
        }
    }
}
