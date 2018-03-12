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
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class RBMBackedTimestampSetAggregatorTest {
    private static final RBMBackedTimestampSetAggregator RBM_BACKED_TIMESTAMP_SET_AGGREGATOR
            = new RBMBackedTimestampSetAggregator();

    @Test
    public void testAggregate() {
        // Given
        final RBMBackedTimestampSet rbmBackedTimestampSet1 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        rbmBackedTimestampSet1.add(Instant.ofEpochMilli(1000L));
        rbmBackedTimestampSet1.add(Instant.ofEpochMilli(1000000L));
        final RBMBackedTimestampSet rbmBackedTimestampSet2 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        rbmBackedTimestampSet2.add(Instant.ofEpochMilli(1000L));
        rbmBackedTimestampSet2.add(Instant.ofEpochMilli(2000000L));

        // When
        final RBMBackedTimestampSet aggregated = RBM_BACKED_TIMESTAMP_SET_AGGREGATOR._apply(rbmBackedTimestampSet1,
                rbmBackedTimestampSet2);
        final RBMBackedTimestampSet expected = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        expected.add(Instant.ofEpochMilli(1000L));
        expected.add(Instant.ofEpochMilli(1000000L));
        expected.add(Instant.ofEpochMilli(2000000L));

        // Then
        assertEquals(3, aggregated.getNumberOfTimestamps());
        assertEquals(expected, aggregated);
    }

    @Test
    public void testCantMergeIfDifferentTimeBucket() {
        try {
            final RBMBackedTimestampSet rbmBackedTimestampSet1 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
            final RBMBackedTimestampSet rbmBackedTimestampSet2 = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
            RBM_BACKED_TIMESTAMP_SET_AGGREGATOR._apply(rbmBackedTimestampSet1, rbmBackedTimestampSet2);
        } catch (final RuntimeException e) {
            // Expected
        }
    }
}
