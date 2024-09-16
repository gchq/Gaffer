/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.predicate;

import org.apache.datasketches.hll.HllSketch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.predicate.PredicateTest;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HllSketchIsLessThanTest extends PredicateTest<HllSketchIsLessThan> {

    private static final double DELTA = 0.00001D;

    private static HllSketch hllSketchWithCardinality5;
    private static HllSketch hllSketchWithCardinality18;
    private static HllSketch hllSketchWithCardinality32;

    @BeforeAll
    public static void setup() {
        hllSketchWithCardinality5 = new HllSketch(10);
        for (int i = 1; i <= 5; i++) {
            hllSketchWithCardinality5.update(i);
        }
        assertEquals(5d, hllSketchWithCardinality5.getEstimate(), DELTA);

        hllSketchWithCardinality18 = new HllSketch(10);
        for (int i = 1; i <= 18; i++) {
            hllSketchWithCardinality18.update(i);
        }
        assertEquals(18d, hllSketchWithCardinality18.getEstimate(), DELTA);

        hllSketchWithCardinality32 = new HllSketch(10);
        for (int i = 1; i <= 32; i++) {
            hllSketchWithCardinality32.update(i);
        }
        assertEquals(32d, hllSketchWithCardinality32.getEstimate(), DELTA);
    }

    @Test
    public void shouldAcceptWhenLessThan() {
        // Given
        final HllSketchIsLessThan filter = new HllSketchIsLessThan(15);
        // When
        boolean accepted = filter.test(hllSketchWithCardinality5);
        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenEqualToAndEqualToIsFalse() {
        // Given
        final HllSketchIsLessThan filter = new HllSketchIsLessThan(18);
        // When
        boolean accepted = filter.test(hllSketchWithCardinality18);
        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldAcceptWhenEqualToAndEqualToIsTrue() {
        // Given
        final HllSketchIsLessThan filter = new HllSketchIsLessThan(18, true);
        // When
        boolean accepted = filter.test(hllSketchWithCardinality18);
        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenMoreThan() {
        // Given
        final HllSketchIsLessThan filter = new HllSketchIsLessThan(15);
        // When
        boolean accepted = filter.test(hllSketchWithCardinality32);
        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenInputIsNull() {
        // Given
        final HllSketchIsLessThan filter = new HllSketchIsLessThan(15);
        // When
        boolean accepted = filter.test(null);
        // Then
        assertFalse(accepted);
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final long controlValue = 15;
        final HllSketchIsLessThan filter = new HllSketchIsLessThan(controlValue);

        // When 1
        final String json = new String(JSONSerialiser.serialise(filter, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n"
                + "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.predicate.HllSketchIsLessThan\",%n"
                + "  \"orEqualTo\" : false,%n" + "  \"value\" : 15%n" + "}"), json);

        // When 2
        final HllSketchIsLessThan deserialisedProperty = JSONSerialiser.deserialise(json.getBytes(),
                HllSketchIsLessThan.class);
        // Then 2
        assertNotNull(deserialisedProperty);
        assertEquals(controlValue, deserialisedProperty.getControlValue(), DELTA);
    }

    @Override
    protected HllSketchIsLessThan getInstance() {
        return new HllSketchIsLessThan(10);
    }

    @Override
    protected Iterable<HllSketchIsLessThan> getDifferentInstancesOrNull() {
        return Arrays.asList(
                new HllSketchIsLessThan(20),
                new HllSketchIsLessThan(20, true)
        );
    }
}
