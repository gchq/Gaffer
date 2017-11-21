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

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StringUtil;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LongTimeSeriesTest extends JSONSerialisationTest<LongTimeSeries> {

    @Test
    public void testPutAndGet() {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MINUTE);
        final Instant instant1 = Instant.ofEpochMilli(1000L * 60);
        final Instant instant2 = Instant.ofEpochMilli(1000L * 60);

        // When
        timeSeries.put(instant1, 100L);
        timeSeries.put(instant2, 100L);

        // Then
        assertEquals(100L, (long) timeSeries.get(instant1));
        assertEquals(100L, (long) timeSeries.get(instant2));
    }

    @Test
    public void testUpsert() {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MINUTE);
        final Instant instant = Instant.ofEpochMilli(1000L * 60);

        // When
        timeSeries.put(instant, 100L);
        timeSeries.upsert(instant, 200L);

        // Then
        assertEquals(300L, (long) timeSeries.get(instant));
    }

    @Test
    public void testBucketAppliedCorrectly() {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MINUTE);
        final Instant instant1 = Instant.ofEpochMilli(1000L * 60);
        final Instant instant2 = Instant.ofEpochMilli(1000L * 60 + 1);

        // When
        timeSeries.put(instant1, 100L);
        timeSeries.upsert(instant2, 200L);

        // Then
        assertEquals(300L, (long) timeSeries.get(instant1));
    }

    @Test
    public void testGetInstants() {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MINUTE);
        final Set<Instant> instantsToInsert = new HashSet<>();
        IntStream.range(1, 11)
                .mapToObj(i -> Instant.ofEpochMilli(1000L * 60 * i))
                .forEach(instantsToInsert::add);
        instantsToInsert.stream()
                .forEach(i -> timeSeries.put(i, 1L));

        // When
        final SortedSet<Instant> instants = timeSeries.getInstants();

        // Then
        assertEquals(instantsToInsert, instants);
    }

    @Test
    public void testGetNumberOfInstants() {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MINUTE);
        IntStream.range(1, 11)
                .mapToObj(i -> Instant.ofEpochMilli(1000L * 60 * i))
                .forEach(i -> timeSeries.put(i, 1L));

        // When
        final int numberOfInstants = timeSeries.getNumberOfInstants();

        // Then
        assertEquals(10, numberOfInstants);
    }

    @Test
    public void testGetTimeBucket() {
        // Given
        final LongTimeSeries timeSeries1 = new LongTimeSeries(TimeBucket.MINUTE);
        final LongTimeSeries timeSeries2 = new LongTimeSeries(TimeBucket.HOUR);

        // When
        final TimeBucket bucket1 = timeSeries1.getTimeBucket();
        final TimeBucket bucket2 = timeSeries2.getTimeBucket();

        // Then
        assertEquals(TimeBucket.MINUTE, bucket1);
        assertEquals(TimeBucket.HOUR, bucket2);
    }

    @Test
    public void testEquals() {
        // Given
        final LongTimeSeries timeSeries1 = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries1.upsert(Instant.ofEpochMilli(1000L), 1000L);
        timeSeries1.upsert(Instant.ofEpochMilli(10000L), 10000L);
        final LongTimeSeries timeSeries2 = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries2.upsert(Instant.ofEpochMilli(1000L), 1000L);
        timeSeries2.upsert(Instant.ofEpochMilli(10000L), 10000L);
        final LongTimeSeries timeSeries3 = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries3.upsert(Instant.ofEpochMilli(1000L), 1000L);
        final LongTimeSeries timeSeries4 = new LongTimeSeries(TimeBucket.SECOND);

        // When
        final boolean shouldBeEqual = timeSeries1.equals(timeSeries2);
        final boolean shouldntBeEqual1 = timeSeries1.equals(timeSeries3);
        final boolean shouldntBeEqual2 = timeSeries1.equals(timeSeries4);

        // Then
        assertEquals(true, shouldBeEqual);
        assertEquals(false, shouldntBeEqual1);
        assertEquals(false, shouldntBeEqual2);
    }

    @Test
    public void testHashcode() {
        // Given
        final LongTimeSeries timeSeries1 = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries1.upsert(Instant.ofEpochMilli(1000L), 1000L);
        timeSeries1.upsert(Instant.ofEpochMilli(10000L), 10000L);
        final int hashCode1 = timeSeries1.hashCode();
        final LongTimeSeries timeSeries2 = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries2.upsert(Instant.ofEpochMilli(1000L), 1000L);
        timeSeries2.upsert(Instant.ofEpochMilli(10000L), 10000L);
        final int hashCode2 = timeSeries2.hashCode();
        final LongTimeSeries timeSeries3 = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries3.upsert(Instant.ofEpochMilli(1000L), 1000L);
        final int hashCode3 = timeSeries3.hashCode();
        final LongTimeSeries timeSeries4 = new LongTimeSeries(TimeBucket.SECOND);
        final int hashCode4 = timeSeries4.hashCode();

        // When
        final boolean shouldBeEqual = hashCode1 == hashCode2;
        final boolean shouldntBeEqual1 = hashCode1 == hashCode3;
        final boolean shouldntBeEqual2 = hashCode1 == hashCode4;

        // Then
        assertEquals(true, shouldBeEqual);
        assertEquals(false, shouldntBeEqual1);
        assertEquals(false, shouldntBeEqual2);
    }

    @Test
    public void testLargeInstants() {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.SECOND);
        timeSeries.upsert(Instant.ofEpochMilli(Long.MAX_VALUE), Long.MAX_VALUE);

        // When
        final long value = timeSeries.get(Instant.ofEpochMilli(Long.MAX_VALUE));

        // Then
        assertEquals(Long.MAX_VALUE, value);
    }

    @Test
    public void testBuilder() {
        // Given
        final Map<Instant, Long> counts = new HashMap<>();
        counts.put(Instant.ofEpochMilli(1000L), 10L);
        counts.put(Instant.ofEpochMilli(100000L), 1000L);
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.SECOND, counts);

        // When
        final long value1 = timeSeries.get(Instant.ofEpochMilli(1000L));
        final long value2 = timeSeries.get(Instant.ofEpochMilli(100000L));

        // Then
        assertEquals(10L, value1);
        assertEquals(1000L, value2);
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final LongTimeSeries obj = getTestObject();

        // When
        final byte[] json = toJson(obj);
        final LongTimeSeries deserialisedObj = fromJson(json);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"timeBucket\" : \"SECOND\",%n" +
                "  \"timeSeries\" : {%n" +
                "    \"1970-01-01T00:00:01Z\" : 10,%n" +
                "    \"1970-01-01T00:01:40Z\" : 1000%n" +
                "  }%n" +
                "}"), StringUtil.toString(json));
        assertNotNull(deserialisedObj);
        assertEquals(obj, deserialisedObj);
    }

    @Test
    public void shouldThrowExceptionIfDeserialisedWithoutTimeBucket() {
        // Given
        final byte[] json = StringUtil.toBytes(String.format("{%n" +
                "  \"timeSeries\" : {%n" +
                "    \"1970-01-01T00:00:01Z\" : 10,%n" +
                "    \"1970-01-01T00:01:40Z\" : 1000%n" +
                "  }%n" +
                "}"));

        // When / Then
        try {
            fromJson(json);
            fail("Exception expected");
        } catch (final Exception e) {
            assertTrue("Error message was: " + e.getMessage(), e.getMessage().contains("A TimeBucket of null is not supported"));
        }
    }

    @Override
    protected LongTimeSeries getTestObject() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.SECOND);
        timeSeries.put(Instant.ofEpochMilli(1000L), 10L);
        timeSeries.put(Instant.ofEpochMilli(100000L), 1000L);
        return timeSeries;
    }
}
