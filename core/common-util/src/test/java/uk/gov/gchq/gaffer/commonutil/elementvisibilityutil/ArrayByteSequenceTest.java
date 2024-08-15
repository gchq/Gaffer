/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * This test class is copied from org.apache.accumulo.core.data.ArrayByteSequenceTest.
 */
class ArrayByteSequenceTest {

    ArrayByteSequence abs;
    byte[] data;

    @BeforeEach
    public void setUp() {
        data = new byte[] {'s', 'm', 'i', 'l', 'e', 's'};
        abs = new ArrayByteSequence(data);
    }

    @Test
    void testInvalidByteBufferBounds0ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs = new ArrayByteSequence(data, -1, 0));
    }

    @Test
    void testInvalidByteBufferBounds1ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs = new ArrayByteSequence(data, data.length + 1, 0));
    }

    @Test
    void testInvalidByteBufferBounds2ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs = new ArrayByteSequence(data, 0, -1));
    }

    @Test
    void testInvalidByteBufferBounds3ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs = new ArrayByteSequence(data, 6, 2));
    }

    @Test
    void testInvalidByteAt0ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs.byteAt(-1));
    }

    @Test
    void testInvalidByteAt1ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs.byteAt(data.length));
    }

    @Test
    void testSubSequence() {
        assertThat(abs.subSequence(0, 0).length()).isZero();
        assertThat(abs.subSequence(1, 5)).hasToString("mile");
    }

    @Test
    void testInvalidSubsequence0ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs.subSequence(5, 1));
    }

    @Test
    void testInvalidSubsequence1ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs.subSequence(-1, 1));
    }

    @Test
    void testInvalidSubsequence3ShouldThrowIAX() {
        assertThatIllegalArgumentException().isThrownBy(() -> abs.subSequence(0, 10));
    }

    @Test
    void testFromByteBuffer() {
        final ByteBuffer bb = ByteBuffer.wrap(data, 1, 4);
        abs = new ArrayByteSequence(bb);

        assertThat(abs).hasToString("mile");
    }

    @Test
    void testFromReadOnlyByteBuffer() {
        final ByteBuffer bb = ByteBuffer.wrap(data, 1, 4).asReadOnlyBuffer();
        abs = new ArrayByteSequence(bb);

        assertThat(abs).hasToString("mile");
    }

    @Test
    void testToString() {
        assertThat(new ArrayByteSequence(""))
            .as("String conversion should round trip correctly")
            .hasToString("");
    }
}
