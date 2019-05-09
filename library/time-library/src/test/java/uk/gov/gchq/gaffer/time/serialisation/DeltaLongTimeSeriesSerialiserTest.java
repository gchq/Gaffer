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
package uk.gov.gchq.gaffer.time.serialisation;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.time.LongTimeSeries;

import java.time.Instant;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeltaLongTimeSeriesSerialiserTest extends ToBytesSerialisationTest<LongTimeSeries> {
    private static final DeltaLongTimeSeriesSerialiser SERIALISER = new DeltaLongTimeSeriesSerialiser();

    @Test
    public void testSerialiser() throws SerialisationException {
        testSerialiser(getExampleValueMillisecond());
        testSerialiser(getExampleValueSecond());
        testSerialiser(getExampleValueMinute());
        testSerialiser(getExampleValueHour());
        testSerialiser(getExampleValueDay());
        testSerialiser(getExampleValueMonth());
        testSerialiser(getExampleValueYear());
    }

    private void testSerialiser(final LongTimeSeries timeSeries) throws SerialisationException {
        // When
        final byte[] serialised = SERIALISER.serialise(timeSeries);
        final LongTimeSeries deserialised = SERIALISER.deserialise(serialised);

        // Then
        assertEquals(timeSeries, deserialised);
        assertEquals(timeSeries.getTimeBucket(), deserialised.getTimeBucket());
        assertEquals(timeSeries.getInstants(), deserialised.getInstants());
        assertEquals(timeSeries.getNumberOfInstants(), deserialised.getNumberOfInstants());
        assertEquals(timeSeries.getTimeSeries(), deserialised.getTimeSeries());
    }

    @Test
    public void testSerialiserForEmptyTimeSeries() throws SerialisationException {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.SECOND);

        // When
        final byte[] serialised = SERIALISER.serialise(timeSeries);
        final LongTimeSeries deserialised = SERIALISER.deserialise(serialised);

        // Then
        assertEquals(timeSeries, deserialised);
    }

    @Test
    public void testValueCloseToLongMax() throws SerialisationException {
        // Given
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.SECOND);
        timeSeries.upsert(Instant.ofEpochMilli(1000L), Long.MAX_VALUE);

        // When
        final byte[] serialised = SERIALISER.serialise(timeSeries);
        final LongTimeSeries deserialised = SERIALISER.deserialise(serialised);

        // Then
        assertEquals(timeSeries, deserialised);
        assertEquals(Long.MAX_VALUE, (long) deserialised.get(Instant.ofEpochMilli(1000L)));
    }

    @Test
    public void testCanHandle() throws SerialisationException {
        assertTrue(SERIALISER.canHandle(LongTimeSeries.class));
        assertFalse(SERIALISER.canHandle(String.class));
    }

    @Test
    public void testConsistent() throws SerialisationException {
        // Given
        final LongTimeSeries timeSeries1 = new LongTimeSeries(TimeBucket.SECOND);
        timeSeries1.upsert(Instant.ofEpochMilli(1000L), 10L);
        final LongTimeSeries timeSeries2 = new LongTimeSeries(TimeBucket.SECOND);
        timeSeries2.upsert(Instant.ofEpochMilli(1000L), 5L);
        timeSeries2.upsert(Instant.ofEpochMilli(1000L), 5L);

        // When
        final byte[] serialised1 = SERIALISER.serialise(timeSeries1);
        final byte[] serialised2 = SERIALISER.serialise(timeSeries2);

        // Then
        assertArrayEquals(serialised1, serialised2);
    }

    @Override
    public Serialiser<LongTimeSeries, byte[]> getSerialisation() {
        return new DeltaLongTimeSeriesSerialiser();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<LongTimeSeries, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair(getExampleValueMillisecond(),
                        new byte[]{7, 3, 1, -114, 3, -24, 1, -114, 7, -48, 9, -115, 15, 54, -120, -115, -104, -106, 118}),
                new Pair(getExampleValueSecond(),
                        new byte[]{0, 3, 1, -115, 1, -122, -96, 100, -115, 13, -69, -96, 0, -117, -24, -44, -107, -51, -64, -116, 59, -102, -55, -100}),
                new Pair(getExampleValueMinute(),
                        new byte[]{1, 3, 1, -115, 91, -115, -128, 100, -116, 35, 103, -72, -128, -114, 38, -84, -117, 13, -44, -124, 18, 0, -115, 15, 27, 48}),
                new Pair(getExampleValueHour(),
                        new byte[]{2, 3, 1, -116, 21, 117, 42, 0, 100, -116, -63, 30, 122, 0, -114, 3, -124, -118, 3, 69, 90, 36, -4, 0, -115, 15, 62, 88}),
                new Pair(getExampleValueDay(),
                        new byte[]{3, 3, 1, -116, 36, 12, -124, 0, 7, -116, 36, 12, -124, 0, 7, -117, 13, -52, -54, -120, 0, -114, 2, -82}),
                new Pair(getExampleValueWeek(),
                        new byte[]{4, 3, 1, -116, 20, -103, 112, 0, 7, -117, 1, 68, 112, -92, 0, 63, -117, 12, -84, 102, 104, 0, -114, 2, 118}),
                new Pair(getExampleValueMonth(),
                        new byte[]{5, 3, 1, 0, 30, -117, 1, -49, 124, 88, 0, 60, -117, 17, -56, 117, -80, 0, -114, 3, -114}),
                new Pair(getExampleValueYear(),
                        new byte[]{6, 3, 1, -118, 57, 88, 37, 75, 88, 0, -114, 7, -48, -117, 7, 92, -41, -120, 0, 0, -117, 117, -113, -84, 48, 0, 0})
        };
    }

    private LongTimeSeries getExampleValueMillisecond() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MILLISECOND);
        timeSeries.upsert(Instant.ofEpochMilli(1_000L), 1L);
        timeSeries.upsert(Instant.ofEpochMilli(3_000L), 10L);
        timeSeries.upsert(Instant.ofEpochMilli(1_000_000L), 10_000_000L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueSecond() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.SECOND);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_SECOND * 100), 100L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_SECOND * 1_000), 100L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_SECOND * 1_000_000_000), 1_000_000_000L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueMinute() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MINUTE);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_MINUTE * 100), 100L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_MINUTE * 10_000), 10_000L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_MINUTE * 1_000_000), 1_000_000L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueHour() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.HOUR);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_HOUR * 100), 100L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_HOUR * 1_000), 1_000L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_HOUR * 1_000_000), 1_000_000L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueDay() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.DAY);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 7), 7L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 14), 14L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 700), 700L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueWeek() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.WEEK);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 7), 7L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 70), 70L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 700), 700L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueMonth() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.MONTH);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 30), 30L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 90), 90L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 1000), 1000L);
        return timeSeries;
    }

    private LongTimeSeries getExampleValueYear() {
        final LongTimeSeries timeSeries = new LongTimeSeries(TimeBucket.YEAR);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 365 * 2000), 2000L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 365 * 2001), 2000L);
        timeSeries.upsert(Instant
                .ofEpochMilli(CommonTimeUtil.MILLISECONDS_IN_DAY * 365 * 2017), 2000L);
        return timeSeries;
    }
}
