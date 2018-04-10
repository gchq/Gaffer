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

package uk.gov.gchq.gaffer.commonutil;

import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility methods for a ByteBuffer.
 * This class is coped from org.apache.accumulo.core.util.ByteBufferUtil.
 */
public final class ByteBufferUtil {

    private ByteBufferUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    public static byte[] toBytes(final ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        if (buffer.hasArray()) {
            // did not use buffer.get() because it changes the position
            return Arrays.copyOfRange(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.limit() + buffer.arrayOffset());
        } else {
            byte[] data = new byte[buffer.remaining()];
            // duplicate inorder to avoid changing position
            buffer.duplicate().get(data);
            return data;
        }
    }

    public static void write(final DataOutput out, final ByteBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            out.write(toBytes(buffer));
        }
    }

    public static String toString(final ByteBuffer bytes) {
        if (bytes.hasArray()) {
            return new String(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining(), UTF_8);
        } else {
            return new String(toBytes(bytes), UTF_8);
        }
    }

    public static Text toText(final ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }

        if (byteBuffer.hasArray()) {
            Text result = new Text();
            result.set(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
            return result;
        } else {
            return new Text(toBytes(byteBuffer));
        }
    }

    public static List<byte[]> toBytesList(final Collection<ByteBuffer> bytesList) {
        if (bytesList == null) {
            return null;
        }
        ArrayList<byte[]> result = new ArrayList<>(bytesList.size());
        for (final ByteBuffer bytes : bytesList) {
            result.add(toBytes(bytes));
        }
        return result;
    }

    public static ByteArrayInputStream toByteArrayInputStream(final ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            return new ByteArrayInputStream(toBytes(buffer));
        }
    }
}
