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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LongUtilTest {
    @Test
    public void shouldGetRandomNumber() {
        // Given
        final long longValue = Longs.fromBytes((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8);
        final int intValue = Ints.fromBytes((byte) -1, (byte) -2, (byte) -3, (byte) -4);

        // When
        final long random = LongUtil.getRandom(longValue, intValue);

        // Then
        assertArrayEquals(new byte[]{(byte) 5, (byte) 6, (byte) 7, (byte) 8, (byte) -1, (byte) -2, (byte) -3, (byte) -4}, Longs.toByteArray(random));
    }

    @Test
    public void shouldGetDifferentTimeBasedRandoms() {
        // Given
        final int n = 100;

        // When
        final Set<Long> timestamps = new HashSet<>(n);
        for (int i = 0; i < n; i++) {
            timestamps.add(LongUtil.getTimeBasedRandom());
        }

        // Then
        assertEquals(n, timestamps.size());
    }
}
