/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.elementvisibilityutil;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * This test class is copied from org.apache.accumulo.core.data.ArrayByteSequenceTest.
 */
public class ArrayByteSequenceTest {

    ArrayByteSequence abs;
    byte[] data;

    @Before
    public void setUp() {
        data = new byte[]{'s', 'm', 'i', 'l', 'e', 's'};
        abs = new ArrayByteSequence(data);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteBufferBounds0() {
        abs = new ArrayByteSequence(data, -1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteBufferBounds1() {
        abs = new ArrayByteSequence(data, data.length + 1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteBufferBounds2() {
        abs = new ArrayByteSequence(data, 0, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteBufferBounds3() {
        abs = new ArrayByteSequence(data, 6, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteAt0() {
        abs.byteAt(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteAt1() {
        abs.byteAt(data.length);
    }

    @Test
    public void testSubSequence() {
        assertEquals(0, abs.subSequence(0, 0).length());
        assertEquals("mile", abs.subSequence(1, 5).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSubsequence0() {
        abs.subSequence(5, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSubsequence1() {
        abs.subSequence(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSubsequence3() {
        abs.subSequence(0, 10);
    }

    @Test
    public void testFromByteBuffer() {
        ByteBuffer bb = ByteBuffer.wrap(data, 1, 4);
        abs = new ArrayByteSequence(bb);

        assertEquals("mile", abs.toString());

        bb = bb.asReadOnlyBuffer();
        abs = new ArrayByteSequence(bb);

        assertEquals("mile", abs.toString());
    }

    @Test
    public void testToString() {
        assertEquals("String conversion should round trip correctly", "", new ArrayByteSequence("").toString());
    }
}
