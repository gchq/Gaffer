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

package uk.gov.gchq.gaffer.commonutil;

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
     * Escapes the provided string so that it no longer contains the
     * Constants.DELIMITER character.
     *
     * @param bytes
     *            the byte array to escape
     * @return the escaped byte array
     */
    public static byte[] escape(final byte[] bytes) {
        final byte[] temp = new byte[2 * bytes.length];
        int currentPosition = 0;
        for (final byte b : bytes) {
            if (b == ESCAPE_CHAR) {
                temp[currentPosition++] = ESCAPE_CHAR;
                temp[currentPosition++] = REPLACEMENT_CHAR;
            } else if (b == DELIMITER) {
                temp[currentPosition++] = ESCAPE_CHAR;
                temp[currentPosition++] = ESCAPE_CHAR;
            } else {
                temp[currentPosition++] = b;
            }
        }
        final byte[] escaped = new byte[currentPosition];
        System.arraycopy(temp, 0, escaped, 0, currentPosition);
        return escaped;
    }

    /**
     * Unescapes the provided byte array - this should only be called on byte
     * arrays that have been through the <code>escape</code> method.
     *
     * @param bytes
     *            the byte array to unescape
     * @return the unescaped byte array
     */
    public static byte[] unEscape(final byte[] bytes) {
        final byte[] temp = new byte[bytes.length];
        int currentPosition = 0;
        boolean isEscaped = false;
        for (final byte b : bytes) {
            if (isEscaped) {
                if (b == REPLACEMENT_CHAR) {
                    temp[currentPosition++] = ESCAPE_CHAR;
                } else if (b == ESCAPE_CHAR) {
                    temp[currentPosition++] = DELIMITER;
                } else {
                    temp[currentPosition++] = b;
                }
                isEscaped = false;
            } else {
                if (b == ESCAPE_CHAR) {
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
