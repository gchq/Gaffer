/*
 * Copyright 2016-2018 Crown Copyright
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

import java.security.InvalidParameterException;
import java.util.Arrays;

/**
 * Removes the 0 byte from a byte array. Preserves ordering.
 */
public final class ByteArrayEscapeUtils {

    public static final byte DELIMITER = (byte) 0;
    public static final byte DELIMITER_PLUS_ONE = (byte) 1;
    private static final byte ESCAPE_CHAR = (byte) 1;
    private static final byte REPLACEMENT_CHAR = (byte) 2;

    private ByteArrayEscapeUtils() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    /**
     * Escapes the provided byte[] so that it no longer contains the
     * Constants.DELIMITER character.
     * After escaping the byte[] the appendAfterEscaping[] is appended unescaped.
     *
     * @param bytes               the byte array to escape
     * @param appendAfterEscaping the bytes to append to the array after being escaped.
     * @return the escaped byte array
     */
    public static byte[] escape(final byte[] bytes, final byte... appendAfterEscaping) {
        final byte[] temp = new byte[(2 * bytes.length) + ((null == appendAfterEscaping) ? 0 : appendAfterEscaping.length)];
        int currentPosition = escape(bytes, temp, 0);
        if (null != appendAfterEscaping) {
            for (final byte b : appendAfterEscaping) {
                temp[currentPosition++] = b;
            }
        }
        return Arrays.copyOfRange(temp, 0, currentPosition);
    }

    private static int escape(final byte[] bytes, final byte[] temp, final int position) {
        int currentPosition = position;
        for (final byte b : bytes) {
            if (ESCAPE_CHAR == b) {
                temp[currentPosition++] = ESCAPE_CHAR;
                temp[currentPosition++] = REPLACEMENT_CHAR;
            } else if (DELIMITER == b) {
                temp[currentPosition++] = ESCAPE_CHAR;
                temp[currentPosition++] = ESCAPE_CHAR;
            } else {
                temp[currentPosition++] = b;
            }
        }
        return currentPosition;
    }

    /**
     * Unescapes the provided byte array - this should only be called on byte
     * arrays that have been through the {@code escape} method.
     *
     * @param bytes the byte array to unescape
     * @return the unescaped byte array
     */
    public static byte[] unEscape(final byte[] bytes) {
        return unEscapeByLength(bytes, 0, bytes.length);
    }

    /**
     * Unescapes the provided byte array - this should only be called on byte
     * arrays that have been through the {@code escape} method.
     *
     * @param allBytes The backing byte array which contains the subset to unEscape
     * @param start    The position to start the unEscape, inclusive.
     * @param end      The position to end the unEscape, exclusive
     * @return the unescaped byte array
     */
    public static byte[] unEscape(final byte[] allBytes, final int start, final int end) {
        return unEscapeByPosition(allBytes, start, end);
    }


    /**
     * Unescapes the provided byte array - this should only be called on byte
     * arrays that have been through the {@code escape} method.
     *
     * @param allBytes The backing byte array which contains the subset to unEscape
     * @param start    The position to start the unEscape, inclusive.
     * @param end      The position to end the unEscape, exclusive
     * @return the unescaped byte array
     */
    public static byte[] unEscapeByPosition(final byte[] allBytes, final int start, final int end) {
        return unEscapeByLength(allBytes, start, end - start);
    }

    /**
     * Unescapes the provided byte array - this should only be called on byte
     * arrays that have been through the {@code escape} method.
     *
     * @param allBytes The backing byte array which contains the subset to unEscape.
     * @param offset   The position to start the unEscape, inclusive
     * @param length   The length of bytes to unEscape
     * @return the unescaped byte array
     */
    public static byte[] unEscapeByLength(final byte[] allBytes, final int offset, final int length) {
        if (allBytes.length < offset + length) {
            throw new InvalidParameterException(String.format("unEscape parameters larger than allByte.length:%d, offset:%d, length:%d", allBytes.length, offset, length));
        }
        final byte[] temp = new byte[length];
        int currentPosition = 0;
        boolean isEscaped = false;

        for (int i = offset; i < allBytes.length && i < offset + length; i++) {
            byte b = allBytes[i];
            if (isEscaped) {
                if (REPLACEMENT_CHAR == b) {
                    temp[currentPosition++] = ESCAPE_CHAR;
                } else if (ESCAPE_CHAR == b) {
                    temp[currentPosition++] = DELIMITER;
                } else {
                    temp[currentPosition++] = b;
                }
                isEscaped = false;
            } else {
                if (ESCAPE_CHAR == b) {
                    isEscaped = true;
                } else {
                    temp[currentPosition++] = b;
                }
            }
        }
        final byte[] unEscaped = new byte[currentPosition];
        System.arraycopy(temp, 0, unEscaped, 0, currentPosition);
        return unEscaped;
    }
}
