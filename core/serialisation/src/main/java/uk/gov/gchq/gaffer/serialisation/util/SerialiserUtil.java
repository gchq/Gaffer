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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public abstract class SerialiserUtil {
    public static int getLastDelimiter(final byte[] allBytes, final byte[] fieldBytes, final int lastDelimiter) {
        return lastDelimiter + CompactRawSerialisationUtils.decodeVIntSize(allBytes[lastDelimiter]) + fieldBytes.length;
    }

    public static byte[] getFieldBytes(final byte[] bytes, final int startPos) throws SerialisationException {
        final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[startPos]);
        final byte[] length = new byte[numBytesForLength];
        System.arraycopy(bytes, startPos, length, 0, numBytesForLength);
        int currentPropLength;
        try {
            currentPropLength = (int) CompactRawSerialisationUtils.readLong(length);
        } catch (final SerialisationException e) {
            throw new SerialisationException("Exception reading length of property");
        }

        return Arrays.copyOfRange(bytes, startPos + numBytesForLength, startPos + numBytesForLength + currentPropLength);
    }

    public static void writeBytes(final byte[] bytes, final ByteArrayOutputStream out)
            throws IOException {
        CompactRawSerialisationUtils.write(bytes.length, out);
        out.write(bytes);
    }
}

