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
package uk.gov.gchq.gaffer.accumulostore.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteUtilsTest {
    private byte[] a;
    private byte[] b;
    private byte[] c;
    private byte[] d;
    private byte[] e;
    private byte[] f;
    private BytesAndRange br1;
    private BytesAndRange br2;

    @Before
    public void setUp() throws Exception {
        a = new byte[]{11, 22, 33, 44, 55};
        b = new byte[]{11, 22, 33, 44, 55};
        c = new byte[]{11, 22, 33, 44};
        d = new byte[]{11, 22, 33, 44, 66};
        e = new byte[]{11, 22, 33, 44, 55, 11, 22, 33, 44, 55};
        f = new byte[]{};
    }

    @Test
    public void shouldPassWithSameByteRangeObject() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br1));
    }

    @Test
    public void shouldPassWithSameArraysAndMatchingRange() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length);
        br2 = new BytesAndRange(a, 0, a.length);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithSameArraysAndMismatchedRange() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length - 1);
        br2 = new BytesAndRange(a, 1, a.length);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithSameArraysAndDifferentLengths() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length - 1);
        br2 = new BytesAndRange(a, 0, a.length);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldPassWithDuplicatedArrays() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length);
        br2 = new BytesAndRange(b, 0, b.length);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldPassWithDifferentSizedBackingArraysButMatchingSelectedRange() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length - 1);
        br2 = new BytesAndRange(c, 0, c.length);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldPassWithOffsets() throws Exception {
        br1 = new BytesAndRange(e, 1, 4);
        br2 = new BytesAndRange(a, 1, 4);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldPassWithSameBackingArrayButDifferentOffsets() throws Exception {
        br1 = new BytesAndRange(e, 0, 5);
        br2 = new BytesAndRange(e, 5, 5);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithSameLengthAndDifferentArrayContent() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length);
        br2 = new BytesAndRange(d, 0, d.length);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithDifferentSizedBackingArrays() throws Exception {
        br1 = new BytesAndRange(a, 0, a.length);
        br2 = new BytesAndRange(c, 0, c.length);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithSameBackingArrayMatchingSelectionWithDifferentOffsetsDifferentLengthsButLargerLengthIsLimitedByEndOfArray() throws Exception {
        br1 = new BytesAndRange(e, 0, 5);
        br2 = new BytesAndRange(e, 5, 9999);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowExceptionFailWithCorrectBackingArraysButInvalidMatchingLength() throws Exception {
        br1 = new BytesAndRange(a, 0, 999);
        br2 = new BytesAndRange(d, 0, 999);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithCorrectBackingArraysButInvalidNotMatchingLength() throws Exception {
        br1 = new BytesAndRange(a, 0, 6666);
        br2 = new BytesAndRange(b, 0, 9999);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowExceptionEmptyArrayWithLength() throws Exception {
        br1 = new BytesAndRange(f, 0, 1);
        br2 = new BytesAndRange(f, 0, 1);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldPassWithTwoEmptyArrays() throws Exception {
        br1 = new BytesAndRange(f, 0, 0);
        br2 = new BytesAndRange(f, 0, 0);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithNullAndEmptyArrays() throws Exception {
        br1 = new BytesAndRange(f, 0, 0);
        br2 = new BytesAndRange(null, 0, 0);
        assertFalse(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithEmptyAndZeroRange() throws Exception {
        br1 = new BytesAndRange(a, 0, 0);
        br2 = new BytesAndRange(f, 0, 0);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }

    @Test
    public void shouldFailWithEmptyAndZeroRangeAndOffset() throws Exception {
        br1 = new BytesAndRange(a, 3, 0);
        br2 = new BytesAndRange(f, 0, 0);
        assertTrue(ByteUtils.areKeyBytesEqual(br1, br2));
    }


}
