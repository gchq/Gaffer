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
package uk.gov.gchq.gaffer.time.binaryoperator;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.time.LongTimeSeries;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class LongTimeSeriesAggregatorTest {
    private static final LongTimeSeriesAggregator LONG_TIME_SERIES_AGGREGATOR
            = new LongTimeSeriesAggregator();

    @Test
    public void testAggregate() {
        // Given
        final LongTimeSeries timeSeries1 = new LongTimeSeries(CommonTimeUtil.TimeBucket.SECOND);
        timeSeries1.put(Instant.ofEpochMilli(1_000L), 100L);
        timeSeries1.put(Instant.ofEpochMilli(10_000L), 200L);
        final LongTimeSeries timeSeries2 = new LongTimeSeries(CommonTimeUtil.TimeBucket.SECOND);
        timeSeries2.put(Instant.ofEpochMilli(1_000L), 100L);
        timeSeries2.put(Instant.ofEpochMilli(10_000L), 200L);
        timeSeries2.put(Instant.ofEpochMilli(100_000L), 500L);

        // When
        final LongTimeSeries aggregated = LONG_TIME_SERIES_AGGREGATOR
                ._apply(timeSeries1, timeSeries2);
        final LongTimeSeries expected = new LongTimeSeries(CommonTimeUtil.TimeBucket.SECOND);
        expected.put(Instant.ofEpochMilli(1_000L), 200L);
        expected.put(Instant.ofEpochMilli(10_000L), 400L);
        expected.put(Instant.ofEpochMilli(100_000L), 500L);

        // Then
        assertEquals(expected, aggregated);
    }

    @Test
    public void testCantMergeIfDifferentTimeBucket() {
        try {
            final LongTimeSeries timeSeries1 = new LongTimeSeries(CommonTimeUtil.TimeBucket.SECOND);
            final LongTimeSeries timeSeries2 = new LongTimeSeries(CommonTimeUtil.TimeBucket.MINUTE);
            LONG_TIME_SERIES_AGGREGATOR._apply(timeSeries1, timeSeries2);
        } catch (final RuntimeException e) {
            // Expected
        }
    }
}
