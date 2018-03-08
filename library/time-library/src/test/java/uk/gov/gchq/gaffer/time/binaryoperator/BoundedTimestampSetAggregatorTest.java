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
package uk.gov.gchq.gaffer.time.binaryoperator;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.time.BoundedTimestampSet;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundedTimestampSetAggregatorTest {
    private static final BoundedTimestampSetAggregator BOUNDED_TIMESTAMP_SET_AGGREGATOR
            = new BoundedTimestampSetAggregator();

    @Test
    public void testAggregateWhenBothInNotFullState() {
        // Given
        final BoundedTimestampSet boundedTimestampSet1 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        boundedTimestampSet1.add(Instant.ofEpochMilli(1000L));
        boundedTimestampSet1.add(Instant.ofEpochMilli(1000000L));
        final BoundedTimestampSet boundedTimestampSet2 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        boundedTimestampSet2.add(Instant.ofEpochMilli(1000L));
        boundedTimestampSet2.add(Instant.ofEpochMilli(2000000L));

        // When
        final BoundedTimestampSet aggregated = BOUNDED_TIMESTAMP_SET_AGGREGATOR._apply(boundedTimestampSet1,
                boundedTimestampSet2);
        final BoundedTimestampSet expected = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        expected.add(Instant.ofEpochMilli(1000L));
        expected.add(Instant.ofEpochMilli(1000000L));
        expected.add(Instant.ofEpochMilli(2000000L));

        // Then
        assertEquals(3, aggregated.getNumberOfTimestamps());
        assertEquals(BoundedTimestampSet.State.NOT_FULL, aggregated.getState());
        assertEquals(expected.getTimestamps(), aggregated.getTimestamps());
    }

    @Test
    public void testAggregateWhenBothInSampleState() {
        // Given
        final BoundedTimestampSet boundedTimestampSet1 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        final Set<Instant> instants1 = new HashSet<>();
        IntStream.range(0, 100)
                .forEach(i -> instants1.add(Instant.ofEpochMilli(i * 1000L)));
        instants1.forEach(boundedTimestampSet1::add);
        final BoundedTimestampSet boundedTimestampSet2 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        final Set<Instant> instants2 = new HashSet<>();
        IntStream.range(50, 150)
                .forEach(i -> instants2.add(Instant.ofEpochMilli(i * 1000L)));
        instants2.forEach(boundedTimestampSet2::add);
        final Set<Instant> allInstants = new HashSet<>(instants1);
        allInstants.addAll(instants2);

        // When
        final BoundedTimestampSet aggregated = BOUNDED_TIMESTAMP_SET_AGGREGATOR._apply(boundedTimestampSet1,
                boundedTimestampSet2);

        // Then
        assertEquals(10, aggregated.getNumberOfTimestamps());
        assertEquals(BoundedTimestampSet.State.SAMPLE, aggregated.getState());
        assertTrue(allInstants.containsAll(aggregated.getTimestamps()));
    }

    @Test
    public void testAggregateWhenAIsInNotFullStateAndBIsInSampleState() {
        // Given
        final BoundedTimestampSet boundedTimestampSet1 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        final Set<Instant> instants1 = new HashSet<>();
        instants1.add(Instant.ofEpochMilli(1000L));
        instants1.add(Instant.ofEpochMilli(1000000L));
        instants1.forEach(boundedTimestampSet1::add);
        final BoundedTimestampSet boundedTimestampSet2 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        final Set<Instant> instants2 = new HashSet<>();
        IntStream.range(50, 150)
                .forEach(i -> instants2.add(Instant.ofEpochMilli(i * 1000L)));
        instants2.forEach(boundedTimestampSet2::add);
        final Set<Instant> allInstants = new HashSet<>(instants1);
        allInstants.addAll(instants2);

        // When
        final BoundedTimestampSet aggregated = BOUNDED_TIMESTAMP_SET_AGGREGATOR._apply(boundedTimestampSet1,
                boundedTimestampSet2);

        // Then
        assertEquals(10, aggregated.getNumberOfTimestamps());
        assertEquals(BoundedTimestampSet.State.SAMPLE, aggregated.getState());
        assertTrue(allInstants.containsAll(aggregated.getTimestamps()));
    }

    @Test
    public void testAggregateWhenAIsInSampleStateAndBIsInNotFullState() {
        // Given
        final BoundedTimestampSet boundedTimestampSet1 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        final Set<Instant> instants1 = new HashSet<>();
        instants1.add(Instant.ofEpochMilli(1000L));
        instants1.add(Instant.ofEpochMilli(1000000L));
        instants1.forEach(boundedTimestampSet1::add);
        final BoundedTimestampSet boundedTimestampSet2 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
        final Set<Instant> instants2 = new HashSet<>();
        IntStream.range(50, 150)
                .forEach(i -> instants2.add(Instant.ofEpochMilli(i * 1000L)));
        instants2.forEach(boundedTimestampSet2::add);
        final Set<Instant> allInstants = new HashSet<>(instants1);
        allInstants.addAll(instants2);

        // When
        final BoundedTimestampSet aggregated = BOUNDED_TIMESTAMP_SET_AGGREGATOR._apply(boundedTimestampSet2,
                boundedTimestampSet1);

        // Then
        assertEquals(10, aggregated.getNumberOfTimestamps());
        assertEquals(BoundedTimestampSet.State.SAMPLE, aggregated.getState());
        assertTrue(allInstants.containsAll(aggregated.getTimestamps()));
    }

    @Test
    public void testCantMergeIfDifferentTimeBucket() {
        try {
            final BoundedTimestampSet boundedTimestampSet1 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
            final BoundedTimestampSet boundedTimestampSet2 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE, 10);
            BOUNDED_TIMESTAMP_SET_AGGREGATOR._apply(boundedTimestampSet1, boundedTimestampSet2);
        } catch (final RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testCantMergeIfDifferentMaxSize() {
        try {
            final BoundedTimestampSet boundedTimestampSet1 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.SECOND, 10);
            final BoundedTimestampSet boundedTimestampSet2 = new BoundedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE, 11);
            BOUNDED_TIMESTAMP_SET_AGGREGATOR._apply(boundedTimestampSet1, boundedTimestampSet2);
        } catch (final RuntimeException e) {
            // Expected
        }
    }
}
