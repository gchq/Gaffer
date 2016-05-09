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

package gaffer.accumulostore.utils;

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
}
