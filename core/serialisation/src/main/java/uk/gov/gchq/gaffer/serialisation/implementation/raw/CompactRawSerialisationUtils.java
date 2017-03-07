/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The methods in this class are used in both {@link CompactRawIntegerSerialiser} and {@link CompactRawLongSerialiser}.
 * They are based on methods from the org.apache.hadoop.io.WritableUtils class in Apache Hadoop. They are implemented
 * as static methods in a utility class so that other classes can use them directly without the need to create a
 * Serialisation class.
 */
public final class CompactRawSerialisationUtils {

    private CompactRawSerialisationUtils() {
    }

    public static byte[] writeLong(final long l) {
        long value = l;
        if (value >= -112 && value <= 127) {
            return new byte[]{(byte) value};
        }
        final byte[] temp = new byte[9];
        int len = -112;
        if (value < 0) {
            value ^= -1L; // take one's complement'
            len = -120;
        }
        long tmp = value;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }
        temp[0] = (byte) len;
        int place = 1;
        len = (len < -120) ? -(len + 120) : -(len + 112);
        for (int idx = len; idx != 0; idx--) {
            final int shiftbits = (idx - 1) * 8;
            final long mask = 0xFFL << shiftbits;
            temp[place++] = (byte) ((value & mask) >> shiftbits);
        }
        final byte[] result = new byte[place];
        System.arraycopy(temp, 0, result, 0, place);
        return result;
    }

    public static long readLong(final byte[] bytes) throws SerialisationException {
        final byte firstByte = bytes[0];
        final int len = decodeVIntSize(firstByte);
        if (len == 1) {
            return (long) firstByte;
        }
        long i = 0;
        short place = 1;
        for (int idx = 0; idx < len - 1; idx++) {
            final byte b = bytes[place++];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }

    /**
     * Writes a long to the provided {@link OutputStream}.
     * NB: This code is very similar to the code in the {@link CompactRawSerialisationUtils#writeLong(long)}
     * method. This violates the DRY principle, but the alternative is to implement the code in the
     * {@link CompactRawSerialisationUtils#writeLong(long)} method by creating a ByteArrayOutputStream from
     * the byte array and then using this method. This approach avoids that expense.
     *
     * @param l      The long to write.
     * @param output The {@link OutputStream} to write data to.
     * @throws SerialisationException if there is an {@link IOException} writing the long.
     */
    public static void write(final long l, final OutputStream output) throws SerialisationException {
        try {
            long value = l;
            if (value >= -112 && value <= 127) {
                output.write((byte) value);
                return;
            }
            int len = -112;
            if (value < 0) {
                value ^= -1L; // take one's complement'
                len = -120;
            }
            long tmp = value;
            while (tmp != 0) {
                tmp = tmp >> 8;
                len--;
            }
            output.write((byte) len);
            len = (len < -120) ? -(len + 120) : -(len + 112);
            for (int idx = len; idx != 0; idx--) {
                final int shiftbits = (idx - 1) * 8;
                final long mask = 0xFFL << shiftbits;
                output.write((byte) ((value & mask) >> shiftbits));
            }
        } catch (final IOException e) {
            throw new SerialisationException("Exception reading bytes", e);
        }
    }

    /**
     * Reads a long from the provided {@link InputStream}. This requires the long to have been written
     * by {@link CompactRawSerialisationUtils#write(long, OutputStream)}.
     * NB: This code is very similar to the code in the {@link CompactRawSerialisationUtils#readLong(byte[])}
     * method. This violates the DRY principle, but the alternative is to implement the code in the
     * {@link CompactRawSerialisationUtils#readLong(byte[])} method by creating a ByteArrayInputStream from
     * the byte array and then using this method. This approach avoids that expense.
     *
     * @param input The {@link InputStream} to read data from.
     * @return The value of the serialised long.
     * @throws SerialisationException if there is an {@link IOException} converting the data to a long.
     */
    public static long read(final InputStream input) throws SerialisationException {
        try {
            final byte firstByte = (byte) input.read();
            final int len = decodeVIntSize(firstByte);
            if (len == 1) {
                return (long) firstByte;
            }
            long i = 0;
            for (int idx = 0; idx < len - 1; idx++) {
                final byte b = (byte) input.read();
                i = i << 8;
                i = i | (b & 0xFF);
            }
            return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
        } catch (final IOException e) {
            throw new SerialisationException("Exception writing bytes", e);
        }
    }

    public static int decodeVIntSize(final byte value) {
        if (value >= -112) {
            return 1;
        } else if (value < -120) {
            return -119 - value;
        }
        return -111 - value;
    }

    private static boolean isNegativeVInt(final byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

}
