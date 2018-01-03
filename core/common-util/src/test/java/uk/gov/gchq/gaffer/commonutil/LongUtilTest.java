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

import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class LongUtilTest {
    @Test
    public void shouldGetRandomNumber() {
        // Given
        final long currentTime = System.currentTimeMillis();
        final String currentTimeBits = Long.toBinaryString(currentTime);

        // When
        final long random = LongUtil.getRandom(currentTime);

        // Then
        // As we don't know the exact current time so just check the first part
        final String randomBits = Long.toBinaryString(random);
        assertEquals(currentTimeBits.substring(currentTimeBits.length() - 30), randomBits.substring(0, 30));
    }

    @Test
    public void shouldGetDifferentPositiveTimeBasedRandoms() {
        // Given
        int n = 1000;

        // When
        final Set<Long> timestamps = new HashSet<>(n);
        for (int i = 0; i < n; i++) {
            timestamps.add(LongUtil.getTimeBasedRandom());
        }

        timestamps.add(LongUtil.getRandom(Long.MAX_VALUE));
        timestamps.add(LongUtil.getRandom(1));
        n += 2;

        // Then
        assertEquals(n, timestamps.size());
        timestamps.forEach(t -> assertTrue("random number was negative " + t, t >= 0L));
    }
}
