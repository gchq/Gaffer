/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;

public class RBMBackedTimestampSetTest extends JSONSerialisationTest<RBMBackedTimestampSet> {
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
    public void shouldSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final RBMBackedTimestampSet boundedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        IntStream.range(0, 20)
                .forEach(i -> {
                    boundedTimestampSet.add(Instant.ofEpochMilli(i * 1000L));
                });

        // When
        final byte[] json = JSONSerialiser.serialise(boundedTimestampSet, true);
        final RBMBackedTimestampSet deserialisedObj = JSONSerialiser.deserialise(json, RBMBackedTimestampSet.class);

        // Then
        assertEquals(boundedTimestampSet, deserialisedObj);
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

    @Test
    public void testEqualsAndHashcode() {
        // Given
        final RBMBackedTimestampSet timestampSet1 = new RBMBackedTimestampSet(TimeBucket.SECOND);
        timestampSet1.add(instant1);
        timestampSet1.add(instant2);
        final RBMBackedTimestampSet timestampSet2 = new RBMBackedTimestampSet(TimeBucket.SECOND);
        timestampSet2.add(instant1);
        timestampSet2.add(instant2);
        final RBMBackedTimestampSet timestampSet3 = new RBMBackedTimestampSet(TimeBucket.SECOND);
        timestampSet3.add(instant1);
        final RBMBackedTimestampSet timestampSet4 = new RBMBackedTimestampSet(TimeBucket.MINUTE);
        timestampSet4.add(instant1);

        // When
        final boolean equal1And2 = timestampSet1.equals(timestampSet2);
        final boolean equal1And3 = timestampSet1.equals(timestampSet3);
        final boolean equal1And4 = timestampSet1.equals(timestampSet4);
        final int hashCode1 = timestampSet1.hashCode();
        final int hashCode2 = timestampSet2.hashCode();
        final int hashCode3 = timestampSet3.hashCode();
        final int hashCode4 = timestampSet4.hashCode();

        // Then
        assertTrue(equal1And2);
        assertFalse(equal1And3);
        assertFalse(equal1And4);
        assertEquals(hashCode1, hashCode2);
        assertNotEquals(hashCode1, hashCode3);
        assertNotEquals(hashCode1, hashCode4);
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
        final Set<Instant> instants = timestampSet.getTimestamps();
        final SortedSet<Long> datesTruncatedToBucket = new TreeSet<>();
        dates.forEach(d -> datesTruncatedToBucket.add(CommonTimeUtil.timeToBucket(d.toEpochMilli(), bucket)));

        // Then
        assertEquals(datesTruncatedToBucket.size(), instants.size());
        final Iterator<Instant> it = instants.iterator();
        for (final long l : datesTruncatedToBucket) {
            assertEquals(Instant.ofEpochMilli(CommonTimeUtil.timeToBucket(l, bucket)), it.next());
        }
    }

    @Override
    protected RBMBackedTimestampSet getTestObject() {
        return new RBMBackedTimestampSet(TimeBucket.SECOND);
    }
}
