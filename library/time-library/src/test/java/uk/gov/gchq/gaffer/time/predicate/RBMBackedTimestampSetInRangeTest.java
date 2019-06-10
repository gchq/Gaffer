/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.time.predicate;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.koryphe.util.JsonSerialiser;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RBMBackedTimestampSetInRangeTest {

    private RBMBackedTimestampSet timestamps;
    private RBMBackedTimestampSetInRange predicate;

    @Before
    public void before() {
        timestamps = new RBMBackedTimestampSet.Builder()
                .timestamps(Lists.newArrayList(
                        secondsAfterEpoch(0L),
                        secondsAfterEpoch(5L),
                        secondsAfterEpoch(10L)
                ))
                .timeBucket(CommonTimeUtil.TimeBucket.SECOND)
                .build();

        predicate = new RBMBackedTimestampSetInRange();
    }

    private Instant secondsAfterEpoch(final Long seconds) {
        return Instant.EPOCH.plusSeconds(seconds);
    }

    @Test
    public void shouldReturnTrueIfNoUpperAndLowerBoundsAreProvided() {
        // When no bounds are provided
        // Then
        assertTrue(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnTrueIfAllTimestampsAreWithinRange() {
        // When
        predicate.start(Instant.EPOCH).end(Instant.now());

        // Then
        assertTrue(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnFalseIfOneTimestampIsOutsideRangeIfIncludeAllTimestampsIsSetToTrue() {
        // Given
        predicate.setIncludeAllTimestamps();

        // When
        predicate.start(Instant.EPOCH.plusSeconds(4L)).end(Instant.now());

        // Then
        assertFalse(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnTrueIfOneTimestampIsOutsideRangeIfIncludeAllTimestampsIsSetToFalse() {
        // Given
        predicate.setIncludeAllTimestamps(false);

        // When
        predicate.start(Instant.EPOCH.plusSeconds(4L)).end(Instant.now());

        // Then
        assertTrue(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnFalseIfNoneOfTheTimestampsAreWithinRange() {
        // When
        predicate.start(Instant.EPOCH.minusSeconds(5L)).end(Instant.EPOCH.plusSeconds(5L));

        // Then
        assertFalse(predicate.test(timestamps));
    }

    @Test
    public void shouldThrowExceptionIfTimestampSetIsNull() {
        // Given
        predicate.start(Instant.EPOCH.minusSeconds(5L)).end(Instant.EPOCH.plusSeconds(5L));

        // When / Then
        try {
            predicate.test(null);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("TimestampSet cannot be null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfTimestampSetIsEmpty() {
        // Given
        predicate.start(Instant.EPOCH.minusSeconds(5L)).end(Instant.EPOCH.plusSeconds(5L));

        // When / Then
        RBMBackedTimestampSet emptySet = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.SECOND)
                .build();
        try {
            predicate.test(emptySet);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("TimestampSet must contain at least one value", e.getMessage());
        }
    }

    @Test
    public void shouldReturnTrueIfTimestampSetContainsValuesAboveLowerBoundWithNoUpperBoundProvided() {
        // Given no end
        // When
        predicate.setStart(Instant.EPOCH.plusSeconds(5L));

        // Then
        assertTrue(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnTrueIfTimestampSetContainsValuesBelowUpperBoundWithNoLowerBoundProvided() {
        // Given no start
        // When
        predicate.setEnd(Instant.EPOCH); // also testing if range is inclusive - it should be

        assertTrue(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnFalseIfTimestampSetContainsNoValuesAboveLowerBoundWithNoUpperBoundProvided() {
        // Given no end
        // When
        predicate.setStart(Instant.now());

        // Then
        assertFalse(predicate.test(timestamps));
    }

    @Test
    public void shouldReturnFalseIfTimestampSetContainsNoValuesBelowUpperBoundWithNoLowerBoundProvided() {
        // Given no start
        // When
        predicate.setEnd(Instant.EPOCH.minusSeconds(20));

        // Then
        assertFalse(predicate.test(timestamps));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        RBMBackedTimestampSetInRange pred = new RBMBackedTimestampSetInRange()
                .start(Instant.EPOCH.plusMillis(10))
                .end(Instant.EPOCH.plusMillis(200))
                .includeAllTimestamps();

        String expectedSerialisedForm = "{" +
                "\"class\":\"uk.gov.gchq.gaffer.time.predicate.RBMBackedTimestampSetInRange\"," +
                "\"start\":{\"java.lang.Long\":10}," +
                "\"end\":{\"java.lang.Long\":200}," +
                "\"includeAllTimestamps\":true" +
            "}";

        assertEquals(expectedSerialisedForm, JsonSerialiser.serialise(pred));
    }

    @Test
    public void shouldSetIncludeAllTimestampsToFalseByDefault() {

    }

    @Test
    public void shouldNotAddIncludeAllTimestampsToJsonIfSetToFalse() {

    }

}
