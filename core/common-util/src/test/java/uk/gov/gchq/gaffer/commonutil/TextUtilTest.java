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

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TextUtilTest {

    @Test
    public void testGetByteBufferReturnsNullWhenTextIsNull() {
        assertThat(TextUtil.getByteBuffer(null)).isNull();
    }

    @Test
    public void testGetByteBuffer() {
        final Text text = new Text("Some text");

        assertEquals(ByteBuffer.wrap(text.getBytes()), TextUtil.getByteBuffer(text));
    }

    @Test
    public void testGetBytes() {
        final Text text = new Text("Some text");

        assertThat(TextUtil.getBytes(text)).hasSize(9);
        assertThat(new Text(TextUtil.getBytes(text)))
                .isEqualTo(new Text("Some text"));
    }
}
