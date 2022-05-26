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

package uk.gov.gchq.gaffer.hadooputil;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test class is copied from org.apache.accumulo.core.util.ByteByfferUtilTest.
 */
public class HadoopByteBufferUtilTest {

    private static final byte[] TEST_DATA_STRING = "0123456789".getBytes(UTF_8);

    @Test
    public void shouldSucceedToTextNullReturnsNull() {
        assertThat(HadoopByteBufferUtil.toText(null)).isNull();
    }

    @Test
    public void shouldSucceedToTextEqual() {
        /* setup */
        final Text expected = new Text("test");
        final ByteBuffer byteBuffer = ByteBuffer.wrap("test".getBytes());

        /* validate */
        assertThat(HadoopByteBufferUtil.toText(byteBuffer)).isEqualTo(expected);
    }
}
