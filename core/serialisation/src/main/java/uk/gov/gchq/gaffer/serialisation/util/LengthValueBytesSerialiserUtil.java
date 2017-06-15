/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.util;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Utility methods for serialising objects to length-value byte arrays.
 */
public abstract class LengthValueBytesSerialiserUtil {
    public static byte[] serialise(final byte[] bytes) throws IOException {
        try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            serialise(bytes, byteStream);
            return byteStream.toByteArray();
        }
    }

    public static void serialise(final byte[] bytes, final ByteArrayOutputStream out)
            throws IOException {
        if (null == bytes || 0 == bytes.length) {
            CompactRawSerialisationUtils.write(0, out);
        } else {
            CompactRawSerialisationUtils.write(bytes.length, out);
            out.write(bytes);
        }
    }

    public static byte[] deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0);
    }

    public static byte[] deserialise(final byte[] bytes, final int startPos) throws SerialisationException {
        if (null == bytes || 0 == bytes.length) {
            return new byte[0];
        }

        final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[startPos]);
        int currentPropLength;
        try (final ByteArrayInputStream input = new ByteArrayInputStream(bytes, startPos, numBytesForLength)) {
            currentPropLength = (int) CompactRawSerialisationUtils.read(input);
        } catch (final IOException e) {
            throw new SerialisationException("Exception reading length of property", e);
        }

        return Arrays.copyOfRange(bytes, startPos + numBytesForLength, startPos + numBytesForLength + currentPropLength);
    }

    public static int getLastDelimiter(final byte[] allBytes, final byte[] fieldBytes, final int lastDelimiter) {
        return lastDelimiter + CompactRawSerialisationUtils.decodeVIntSize(allBytes[lastDelimiter]) + fieldBytes.length;
    }
}

