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

import com.google.common.primitives.Longs;

import java.util.SplittableRandom;

/**
 * Utility methods for Longs.
 */
public final class LongUtil {
    private static final SplittableRandom RANDOM = new SplittableRandom();

    private LongUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Gets a random long value based on the current time and a random number.
     * Made up as follows:
     * <ul style="list-style-type: none;">
     * <li>If the current time bytes are: [a,b,c,d,e,f,h,i]
     * <li>and the random number bytes are: [j,k,l,m]</li>
     * <li>the result bytes would be: [e,f,h,i,j,k,l,m]</li>
     * </ul>
     *
     * @return the time based random number
     */
    public static long getTimeBasedRandom() {
        long time = System.currentTimeMillis();
        int randomVal = RANDOM.nextInt();

        return getRandom(time, randomVal);
    }

    /**
     * Gets a random long value based on a long and an int.
     * Made up as follows:
     * <ul style="list-style-type: none;">
     * <li>If the long bytes are: [a,b,c,d,e,f,h,i]
     * <li>and the int bytes are: [j,k,l,m]</li>
     * <li>the result bytes would be: [e,f,h,i,j,k,l,m]</li>
     * </ul>
     *
     * @param longInput the long input
     * @param intInput  the int input
     * @return the random number
     */
    public static long getRandom(final long longInput, final int intInput) {
        long longValue = longInput;
        final byte b4 = (byte) (longValue & 0xffL);
        longValue >>= 8;
        final byte b3 = (byte) (longValue & 0xffL);
        longValue >>= 8;
        final byte b2 = (byte) (longValue & 0xffL);
        longValue >>= 8;
        final byte b1 = (byte) (longValue & 0xffL);

        final byte b5 = (byte) (intInput >> 24);
        final byte b6 = (byte) (intInput >> 16);
        final byte b7 = (byte) (intInput >> 8);
        final byte b8 = (byte) intInput;

        return Longs.fromBytes(b1, b2, b3, b4, b5, b6, b7, b8);
    }
}
