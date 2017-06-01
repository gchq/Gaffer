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
package uk.gov.gchq.gaffer.time;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;

/**
 *
 */
public class RBMBackedTimestampSetTest {
    private SortedSet<Instant> instants = new TreeSet<>();
    private Instant instant1;
    private Instant instant2;

    @Before
    public void setup() {
        instant1 = Instant.now();
        instant2 = instant1.plus(Duration.ofDays(100L));
        instants.add(instant1);
        instants.add(instant2);
    }

    @Test
    public void testGet() {
        testGet(instants);

        final SortedSet<Instant> randomDates = new TreeSet<>();
        IntStream.range(0, 100)
                .forEach(i -> randomDates.add(Instant.ofEpochMilli(instant1.toEpochMilli() + i * 12345678L)));
        testGet(randomDates);
    }

    @Test
    public void testGetEarliestAndGetLatest() {
        // Given
        final RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(TimeBucket.SECOND);
        timestampSet.add(instant1);
        timestampSet.add(instant2);

        // When
        final Instant earliest = timestampSet.getEarliest();
        final Instant latest = timestampSet.getLatest();

        // Then
        assertEquals(Instant.ofEpochMilli(CommonTimeUtil.timeToBucket(instant1.toEpochMilli(), TimeBucket.SECOND)), earliest);
        assertEquals(Instant.ofEpochMilli(CommonTimeUtil.timeToBucket(instant2.toEpochMilli(), TimeBucket.SECOND)), latest);
    }

    @Test
    public void testGetNumberOfTimestamps() {
        // Given
        final RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(TimeBucket.SECOND);
        timestampSet.add(instant1);
        timestampSet.add(instant1.plus(Duration.ofDays(100L)));
        timestampSet.add(instant1.plus(Duration.ofDays(200L)));
        timestampSet.add(instant1.plus(Duration.ofDays(300L)));
        // Add another instant that should be truncated to the same as the previous one
        timestampSet.add(instant1.plus(Duration.ofDays(300L)).plusMillis(1L));

        // When
        final long numberOfTimestamps = timestampSet.getNumberOfTimestamps();

        // Then
        assertEquals(4, numberOfTimestamps);
    }

    private void testGet(final SortedSet<Instant> dates) {
        testGet(dates, TimeBucket.SECOND);
        testGet(dates, TimeBucket.MINUTE);
        testGet(dates, TimeBucket.HOUR);
        testGet(dates, TimeBucket.DAY);
        testGet(dates, TimeBucket.MONTH);
    }

    private void testGet(final SortedSet<Instant> dates, final TimeBucket bucket) {
        // Given
        RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(bucket);
        dates.forEach(d -> timestampSet.add(d));

        // When
        final Set<Instant> instants = timestampSet.get();
        final SortedSet<Long> datesTruncatedToBucket = new TreeSet<>();
        dates.forEach(d -> datesTruncatedToBucket.add(CommonTimeUtil.timeToBucket(d.toEpochMilli(), bucket)));

        // Then
        assertEquals(datesTruncatedToBucket.size(), instants.size());
        final Iterator it = instants.iterator();
        for (final long l : datesTruncatedToBucket) {
            assertEquals(Instant.ofEpochMilli(CommonTimeUtil.timeToBucket(l, bucket)), it.next());
        }
    }
}
