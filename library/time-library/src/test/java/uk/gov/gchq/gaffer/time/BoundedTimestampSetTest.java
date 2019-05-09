/*
 * Copyright 2017-2019 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundedTimestampSetTest extends JSONSerialisationTest<BoundedTimestampSet> {

    @Test
    public void shouldSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final BoundedTimestampSet boundedTimestampSet = getTestObject();
        IntStream.range(0, 20)
                .forEach(i -> {
                    boundedTimestampSet.add(Instant.ofEpochMilli(i * 1000L));
                    if (i <= 9) {
                        assertEquals(BoundedTimestampSet.State.NOT_FULL, boundedTimestampSet.getState());
                    } else {
                        assertEquals(BoundedTimestampSet.State.SAMPLE, boundedTimestampSet.getState());
                    }
                });


        // When
        final byte[] json = JSONSerialiser.serialise(boundedTimestampSet, true);
        final BoundedTimestampSet deserialisedObj = JSONSerialiser.deserialise(json, BoundedTimestampSet.class);

        // Then
        assertEquals(boundedTimestampSet, deserialisedObj);
    }

    @Test
    public void testStateTransitionsCorrectly() throws SerialisationException {
        // Given
        final BoundedTimestampSet boundedTimestampSet = getTestObject();

        // When / Then
        IntStream.range(0, 20)
                .forEach(i -> {
                    boundedTimestampSet.add(Instant.ofEpochMilli(i * 1000L));
                    if (i <= 9) {
                        assertEquals(BoundedTimestampSet.State.NOT_FULL, boundedTimestampSet.getState());
                    } else {
                        assertEquals(BoundedTimestampSet.State.SAMPLE, boundedTimestampSet.getState());
                    }
                });
    }

    @Test
    public void testGetWhenNotFull() {
        // Given
        final Instant instant1 = Instant.ofEpochMilli(1000L);
        final Instant instant2 = Instant.ofEpochMilli(1000000L);
        final Set<Instant> instants = new HashSet<>();
        instants.add(instant1);
        instants.add(instant2);
        final BoundedTimestampSet boundedTimestampSet = getTestObject();
        boundedTimestampSet.add(instant1);
        boundedTimestampSet.add(instant2);

        // When
        final SortedSet<Instant> returnedInstants = boundedTimestampSet.getTimestamps();
        final SortedSet<Long> instantsTruncatedToBucket = new TreeSet<>();
        instants.forEach(i -> instantsTruncatedToBucket.add(CommonTimeUtil.timeToBucket(i.toEpochMilli(),
                CommonTimeUtil.TimeBucket.SECOND)));

        // Then
        assertEquals(instantsTruncatedToBucket.size(), returnedInstants.size());
        final Iterator<Instant> it = instants.iterator();
        for (final long l : instantsTruncatedToBucket) {
            assertEquals(Instant.ofEpochMilli(CommonTimeUtil.timeToBucket(l, CommonTimeUtil.TimeBucket.SECOND)), it.next());
        }
    }

    @Test
    public void testGetWhenSampling() {
        // Given
        final Set<Instant> instants = new HashSet<>();
        IntStream.range(0, 1000)
                .forEach(i -> instants.add(Instant.ofEpochMilli(i * 1000L)));
        final BoundedTimestampSet boundedTimestampSet = getTestObject();
        instants.forEach(boundedTimestampSet::add);

        // When
        final SortedSet<Instant> returnedInstants = boundedTimestampSet.getTimestamps();

        // Then
        assertEquals(BoundedTimestampSet.State.SAMPLE, boundedTimestampSet.getState());
        assertEquals(10L, boundedTimestampSet.getNumberOfTimestamps());
        returnedInstants.forEach(i -> assertTrue(instants.contains(i)));
    }

    @Test
    public void testGetEarliestAndGetLatestWhenNotFull() {
        // Given
        final RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        timestampSet.add(Instant.ofEpochMilli(1000L));
        timestampSet.add(Instant.ofEpochMilli(2000L));

        // When
        final Instant earliest = timestampSet.getEarliest();
        final Instant latest = timestampSet.getLatest();

        // Then
        assertEquals(Instant.ofEpochMilli(1000L), earliest);
        assertEquals(Instant.ofEpochMilli(2000L), latest);
    }

    @Test
    public void testGetEarliestAndLatestWhenSampling() {
        // Given
        final Set<Instant> instants = new HashSet<>();
        IntStream.range(0, 1000)
                .forEach(i -> instants.add(Instant.ofEpochMilli(i * 1000L)));
        final BoundedTimestampSet boundedTimestampSet = getTestObject();
        instants.forEach(boundedTimestampSet::add);

        // When
        final Instant earliest = boundedTimestampSet.getEarliest();
        final Instant latest = boundedTimestampSet.getLatest();

        // Then
        assertEquals(BoundedTimestampSet.State.SAMPLE, boundedTimestampSet.getState());
        assertTrue(earliest.isBefore(latest));
        assertTrue(instants.contains(earliest));
        assertTrue(instants.contains(latest));
    }

    @Test
    public void testGetNumberOfTimestampsWhenNotFull() {
        // Given
        final BoundedTimestampSet timestampSet = getTestObject();
        final Instant instant = Instant.ofEpochMilli(1000L);
        timestampSet.add(instant);
        timestampSet.add(instant.plus(Duration.ofDays(100L)));
        timestampSet.add(instant.plus(Duration.ofDays(200L)));
        timestampSet.add(instant.plus(Duration.ofDays(300L)));
        // Add another instant that should be truncated to the same as the previous one
        timestampSet.add(instant.plus(Duration.ofDays(300L)).plusMillis(1L));

        // When
        final long numberOfTimestamps = timestampSet.getNumberOfTimestamps();

        // Then
        assertEquals(4, numberOfTimestamps);
    }

    @Test
    public void testGetNumberOfTimestampsWhenSampling() {
        // Given
        final Set<Instant> instants = new HashSet<>();
        IntStream.range(0, 1000)
                .forEach(i -> instants.add(Instant.ofEpochMilli(i * 1000L)));
        final BoundedTimestampSet boundedTimestampSet = getTestObject();
        instants.forEach(boundedTimestampSet::add);

        // When
        final long numberOfTimestamps = boundedTimestampSet.getNumberOfTimestamps();

        // Then
        assertEquals(10, numberOfTimestamps);
    }

    @Override
    protected BoundedTimestampSet getTestObject() {
        return new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
    }
}
