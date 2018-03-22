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

import java.nio.ByteBuffer;

/**
 * Utility methods for Text.
 * This class is coped from org.apache.accumulo.core.util.TextUtil.
 */
public final class TextUtil {

    private TextUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    public static byte[] getBytes(final Text text) {
        byte[] bytes = text.getBytes();
        if (bytes.length != text.getLength()) {
            bytes = new byte[text.getLength()];
            System.arraycopy(text.getBytes(), 0, bytes, 0, bytes.length);
        }
        return bytes;
    }

    public static ByteBuffer getByteBuffer(final Text text) {
        if (text == null) {
            return null;
        }
        byte[] bytes = text.getBytes();
        return ByteBuffer.wrap(bytes, 0, text.getLength());
    }
}
