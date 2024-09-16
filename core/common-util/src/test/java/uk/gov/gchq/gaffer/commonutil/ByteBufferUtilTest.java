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

package uk.gov.gchq.gaffer.commonutil;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ArrayByteSequence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test class is copied from org.apache.accumulo.core.util.ByteByfferUtilTest.
 */
class ByteBufferUtilTest {

    private static final byte[] TEST_DATA_STRING = "0123456789".getBytes(UTF_8);

    @Test
    void testNonZeroArrayOffset() {
        final ByteBuffer bb1 = ByteBuffer.wrap(TEST_DATA_STRING, 3, 4);

        // create a ByteBuffer with a non-zero array offset
        final ByteBuffer bb2 = bb1.slice();

        // The purpose of this test is to ensure ByteBufferUtil code works when arrayOffset is non-zero. The following asserts are not to test ByteBuffer, but
        // ensure the behaviour of slice() is as expected.

        assertThat(bb2.arrayOffset()).isEqualTo(3);
        assertThat(bb2.position()).isZero();
        assertThat(bb2.limit()).isEqualTo(4);

        // start test with non zero arrayOffset
        assertByteBufferEquals("3456", bb2);

        // read one byte from byte buffer... this should cause position to be non-zero in addition to array offset
        bb2.get();
        assertByteBufferEquals("456", bb2);
    }

    @Test
    void testZeroArrayOffsetAndNonZeroPosition() {
        final ByteBuffer bb = ByteBuffer.wrap(TEST_DATA_STRING, 3, 4);

        assertByteBufferEquals("3456", bb);
    }

    @Test
    void testZeroArrayOffsetAndPosition() {
        final ByteBuffer bb = ByteBuffer.wrap(TEST_DATA_STRING, 0, 4);

        assertByteBufferEquals("0123", bb);
    }

    @Test
    void testDirectByteBuffer() {
        // allocate direct so it does not have a backing array
        final ByteBuffer bb = ByteBuffer.allocateDirect(10);
        bb.put(TEST_DATA_STRING);
        bb.rewind();

        assertByteBufferEquals("0123456789", bb);

        // advance byte buffer position
        bb.get();
        assertByteBufferEquals("123456789", bb);
    }

    private static void assertByteBufferEquals(final String expected, final ByteBuffer bb) {
        assertThat(ByteBufferUtil.toString(bb)).isEqualTo(expected);
        assertThat(new String(ByteBufferUtil.toBytes(bb), UTF_8)).isEqualTo(expected);

        List<byte[]> bal = ByteBufferUtil.toBytesList(Collections.singletonList(bb));
        assertThat(bal).hasSize(1);
        assertThat(new String(bal.get(0), UTF_8)).isEqualTo(expected);

        assertThat(new ArrayByteSequence(bb)).isEqualTo(new ArrayByteSequence(expected));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            ByteBufferUtil.write(dos, bb);
            dos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertThat(new String(baos.toByteArray(), UTF_8)).isEqualTo(expected);

        ByteArrayInputStream bais = ByteBufferUtil.toByteArrayInputStream(bb);
        byte[] buffer = new byte[expected.length()];
        try {
            bais.read(buffer);
            assertThat(new String(buffer, UTF_8)).isEqualTo(expected);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
