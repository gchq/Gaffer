/*
 * Copyright 2017-2021 Crown Copyright
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

import org.apache.hadoop.io.Text;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test class is copied from org.apache.accumulo.core.util.ByteByfferUtilTest.
 */
public class ByteBufferUtilTest {

    private static final byte[] TEST_DATA_STRING = "0123456789".getBytes(UTF_8);

    @Test
    public void testNonZeroArrayOffset() {
        final ByteBuffer bb1 = ByteBuffer.wrap(TEST_DATA_STRING, 3, 4);

        // create a ByteBuffer with a non-zero array offset
        final ByteBuffer bb2 = bb1.slice();

        // The purpose of this test is to ensure ByteBufferUtil code works when arrayOffset is non-zero. The following asserts are not to test ByteBuffer, but
        // ensure the behavior of slice() is as expected.

        assertEquals(3, bb2.arrayOffset());
        assertEquals(0, bb2.position());
        assertEquals(4, bb2.limit());

        // start test with non zero arrayOffset
        assertByteBufferEquals("3456", bb2);

        // read one byte from byte buffer... this should cause position to be non-zero in addition to array offset
        bb2.get();
        assertByteBufferEquals("456", bb2);
    }

    @Test
    public void testZeroArrayOffsetAndNonZeroPosition() {
        final ByteBuffer bb = ByteBuffer.wrap(TEST_DATA_STRING, 3, 4);

        assertByteBufferEquals("3456", bb);
    }

    @Test
    public void testZeroArrayOffsetAndPosition() {
        final ByteBuffer bb = ByteBuffer.wrap(TEST_DATA_STRING, 0, 4);

        assertByteBufferEquals("0123", bb);
    }

    @Test
    public void testDirectByteBuffer() {
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
        assertEquals(new Text(expected), ByteBufferUtil.toText(bb));
        assertEquals(expected, new String(ByteBufferUtil.toBytes(bb), UTF_8));
        assertEquals(expected, ByteBufferUtil.toString(bb));

        List<byte[]> bal = ByteBufferUtil.toBytesList(Collections.singletonList(bb));
        assertThat(bal).hasSize(1);
        assertEquals(expected, new String(bal.get(0), UTF_8));

        assertEquals(new ArrayByteSequence(expected), new ArrayByteSequence(bb));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            ByteBufferUtil.write(dos, bb);
            dos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertEquals(expected, new String(baos.toByteArray(), UTF_8));

        ByteArrayInputStream bais = ByteBufferUtil.toByteArrayInputStream(bb);
        byte[] buffer = new byte[expected.length()];
        try {
            bais.read(buffer);
            assertEquals(expected, new String(buffer, UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
