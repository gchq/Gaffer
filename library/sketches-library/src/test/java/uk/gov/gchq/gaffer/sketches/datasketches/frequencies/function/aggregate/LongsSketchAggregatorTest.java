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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.function.aggregate;

import com.yahoo.sketches.frequencies.LongsSketch;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class LongsSketchAggregatorTest extends AggregateFunctionTest {
    private LongsSketch sketch1;
    private LongsSketch sketch2;

    @Before
    public void setup() {
        sketch1 = new LongsSketch(32);
        sketch1.update(1L);
        sketch1.update(2L);
        sketch1.update(3L);

        sketch2 = new LongsSketch(32);
        sketch2.update(4L);
        sketch2.update(5L);
        sketch2.update(6L);
        sketch2.update(7L);
        sketch2.update(3L);
    }

    @Test
    public void testAggregate() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();
        sketchAggregator.init();

        sketchAggregator._aggregate(sketch1);
        LongsSketch currentState = sketchAggregator._state();
        assertEquals(1L, currentState.getEstimate(1L));

        sketchAggregator._aggregate(sketch2);
        currentState = sketchAggregator._state();
        assertEquals(1L, currentState.getEstimate(1L));
        assertEquals(2L, currentState.getEstimate(3L));
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();
        sketchAggregator.init();
        sketchAggregator._aggregate(sketch1);
        try {
            sketchAggregator.aggregate(null);
        } catch (final IllegalArgumentException exception) {
            assertEquals("Expected an input array of length 1", exception.getMessage());
        }
    }

    @Test
    public void testFailedExecuteDueToEmptyInput() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();
        sketchAggregator.init();
        sketchAggregator._aggregate(sketch1);
        try {
            sketchAggregator.aggregate(new Object[0]);
        } catch (final IllegalArgumentException exception) {
            assertEquals("Expected an input array of length 1", exception.getMessage());
        }
    }

    @Test
    public void testClone() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();
        sketchAggregator.init();
        sketchAggregator._aggregate(sketch1);
        final LongsSketchAggregator clone = sketchAggregator.statelessClone();
        assertNotSame(sketchAggregator, clone);
        clone._aggregate(sketch2);
        assertEquals(sketch2.getEstimate(1L), ((LongsSketch) clone.state()[0]).getEstimate(1L));
    }

    @Test
    public void testCloneWhenEmpty() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();
        sketchAggregator.init();
        final LongsSketchAggregator clone = sketchAggregator.statelessClone();
        assertNotSame(sketchAggregator, clone);
        clone._aggregate(sketch1);
        assertEquals(sketch1.getEstimate(1L), ((LongsSketch) clone.state()[0]).getEstimate(1L));
    }

    @Test
    public void testCloneOfBusySketch() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();
        sketchAggregator.init();
        for (int i = 0; i < 100; i++) {
            final LongsSketch union = new LongsSketch(32);
            for (int j = 0; j < 100; j++) {
                union.update(j);
            }
            sketchAggregator._aggregate(union);
        }
        final LongsSketchAggregator clone = sketchAggregator.statelessClone();
        assertNotSame(sketchAggregator, clone);
        clone._aggregate(sketch1);
        assertEquals(sketch1.getEstimate(1L), ((LongsSketch) clone.state()[0]).getEstimate(1L));
    }

    @Test
    public void testEquals() {
        final LongsSketch sketch1 = new LongsSketch(32);
        sketch1.update(1L);
        final LongsSketchAggregator sketchAggregator1 = new LongsSketchAggregator();
        sketchAggregator1.aggregate(new LongsSketch[]{sketch1});

        final LongsSketch sketch2 = new LongsSketch(32);
        sketch2.update(1L);
        final LongsSketchAggregator sketchAggregator2 = new LongsSketchAggregator();
        sketchAggregator2.aggregate(new LongsSketch[]{sketch2});

        assertEquals(sketchAggregator1, sketchAggregator2);

        sketch2.update(2L);
        sketchAggregator2.aggregate(new LongsSketch[]{sketch2});
        assertNotEquals(sketchAggregator1, sketchAggregator2);
    }

    @Test
    public void testEqualsWhenEmpty() {
        assertEquals(new LongsSketchAggregator(), new LongsSketchAggregator());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final LongsSketchAggregator aggregator = new LongsSketchAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.frequencies.function.aggregate.LongsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final LongsSketchAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), LongsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return LongsSketchAggregator.class;
    }

    @Override
    protected LongsSketchAggregator getInstance() {
        return new LongsSketchAggregator();
    }
}
