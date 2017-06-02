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

public final class ByteCopyingUtil {
    private ByteCopyingUtil() {
        //empty
    }

    /**
     * into[] = {first[], delimiter, flag, delimiter, second[]}
     *
     * @param first         first Bytes to copy
     * @param second        second bytes to copy
     * @param into          bytes being written to.
     * @param directionFlag direction flag
     * @return returns the carriage for the array bytes where copied into.
     */
    public static int copyFirstAndSecondByteArrayDelimitedWithFlag(final byte[] first, final byte[] second, final byte[] into, final byte directionFlag) {
        return copyFirstAndSecondByteArrayDelimitedWithFlag(first, second, into, directionFlag, 0);
    }

    /**
     * into[] = {..., first[], delimiter, flag, delimiter, second[]}
     *
     * @param first         first Bytes to copy
     * @param second        second bytes to copy
     * @param into          bytes being written to.
     * @param directionFlag direction flag
     * @param destPos       Position or offset to start writing at.
     * @return returns the carriage for the array bytes where copied into.
     */
    public static int copyFirstAndSecondByteArrayDelimitedWithFlag(final byte[] first, final byte[] second, final byte[] into, final byte directionFlag, final int destPos) {
        System.arraycopy(first, 0, into, destPos, first.length);
        int carriage = first.length;
        into[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        into[carriage++] = directionFlag;
        into[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(second, 0, into, carriage, second.length);
        carriage += second.length;
        return carriage;
    }
}
