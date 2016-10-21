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

package uk.gov.gchq.gaffer.accumulostore.utils;

/**
 * Utility methods for bytes
 */
public final class ByteUtils {
    private ByteUtils() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    public static int compareBytes(final byte[] bytes1, final byte[] bytes2) {
        final int minLength = Math.min(bytes1.length, bytes2.length);
        for (int i = 0; i < minLength; i++) {
            if (bytes1[i] != bytes2[i]) {
                return Byte.compare(bytes1[i], bytes2[i]);
            }
        }

        return bytes1.length - bytes2.length;
    }

    /**
     * Copy of the isEqual method in {@link org.apache.accumulo.core.data.Key}.
     *
     * @param bytes1 first array of bytes to test
     * @param bytes2 second array of bytes to test
     * @return true if the provided bytes are equal
     */
    public static boolean areKeyBytesEqual(final byte[] bytes1, final byte[] bytes2) {
        if (bytes1 == bytes2) {
            return true;
        }

        int last = bytes1.length;

        if (last != bytes2.length) {
            return false;
        }

        if (last == 0) {
            return true;
        }

        // since sorted data is usually compared in accumulo,
        // the prefixes will normally be the same... so compare
        // the last two charachters first.. the most likely place
        // to have disorder is at end of the strings when the
        // data is sorted... if those are the same compare the rest
        // of the data forward... comparing backwards is slower
        // (compiler and cpu optimized for reading data forward)..
        // do not want slower comparisons when data is equal...
        // sorting brings equals data together

        last--;

        if (bytes1[last] == bytes2[last]) {
            for (int i = 0; i < last; i++) {
                if (bytes1[i] != bytes2[i]) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }
}
