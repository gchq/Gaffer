/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.utils;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ByteArrayEscapeUtilsTest {

    private static final byte[] EMPTY_BYTES = new byte[0];
    private final static byte ESCAPE_CHAR = (byte) 1;
    private final static byte REPLACEMENT_CHAR = (byte) 2;

    @Test
    public void testNoDelims() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30};
        check(bytes);
    }

    @Test
    public void testWithOneDelim() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ByteArrayEscapeUtils.DELIMITER, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithConsecutiveDelims() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER, (byte) 40, (byte) 50};
        check(bytes);
        bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithEscapeCharacter() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithConsecutiveEscapeCharacter() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, ESCAPE_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, ESCAPE_CHAR, ESCAPE_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithReplacementCharacter() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithConsecutiveReplacementCharacter() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, REPLACEMENT_CHAR, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testWithEscapeThenReplacementCharacter() {
        byte[] bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
        bytes = new byte[]{(byte) 10, (byte) 20, (byte) 30, ESCAPE_CHAR, ESCAPE_CHAR, REPLACEMENT_CHAR, REPLACEMENT_CHAR, (byte) 40, (byte) 50};
        check(bytes);
    }

    @Test
    public void testRandom() {
        for (int i = 0; i < 100000; i++) {
            int length = RandomUtils.nextInt(100) + 1;
            byte[] b = new byte[length];
            for (int j = 0; j < b.length; j++) {
                b[j] = (byte) RandomUtils.nextInt();
            }
            check(b);
        }
    }

    @Test
    public void testOrdering() {
        // Generate some keys with row key formed from random bytes, and add to ordered set
        SortedSet<Key> original = new TreeSet<>();
        for (int i = 0; i < 100000; i++) {
            int length = RandomUtils.nextInt(100) + 1;
            byte[] b = new byte[length];
            for (int j = 0; j < b.length; j++) {
                b[j] = (byte) RandomUtils.nextInt();
            }
            Key key = new Key(b, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
            original.add(key);
        }
        // Loop through set, check that ordering is preserved after escaping
        Iterator<Key> it = original.iterator();
        Key first = it.next();
        Key second = it.next();
        while (true) {
            assertTrue(first.compareTo(second) < 0);
            Key escapedFirst = new Key(ByteArrayEscapeUtils.escape(first.getRowData().getBackingArray()), EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
            Key escapedSecond = new Key(ByteArrayEscapeUtils.escape(second.getRowData().getBackingArray()), EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
            assertTrue(escapedFirst.compareTo(escapedSecond) < 0);
            first = second;
            if (it.hasNext()) {
                second = it.next();
            } else {
                break;
            }
        }
    }

    private static void check(final byte[] bytes) {
        byte[] escaped = ByteArrayEscapeUtils.escape(bytes);
        byte[] unescaped = ByteArrayEscapeUtils.unEscape(escaped);
        assertArrayEquals(bytes, unescaped);
        for (byte anEscaped : escaped) {
            assertNotEquals(ByteArrayEscapeUtils.DELIMITER, anEscaped);
        }
    }
}
