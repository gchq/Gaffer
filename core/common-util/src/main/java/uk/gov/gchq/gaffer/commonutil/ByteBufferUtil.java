/*
 * Copyright 2017 Crown Copyright
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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
        } else if (buffer.hasArray()) {
            return Arrays.copyOfRange(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.limit() + buffer.arrayOffset());
        } else {
            byte[] data = new byte[buffer.remaining()];
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
}
