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

import com.yahoo.sketches.frequencies.ItemsSketch;
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

public class StringsSketchAggregatorTest extends AggregateFunctionTest {
    private ItemsSketch<String> sketch1;
    private ItemsSketch<String> sketch2;

    @Before
    public void setup() {
        sketch1 = new ItemsSketch<>(32);
        sketch1.update("1");
        sketch1.update("2");
        sketch1.update("3");

        sketch2 = new ItemsSketch<>(32);
        sketch2.update("4");
        sketch2.update("5");
        sketch2.update("6");
        sketch2.update("7");
        sketch2.update("3");
    }

    @Test
    public void testAggregate() {
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();
        sketchAggregator.init();

        sketchAggregator._aggregate(sketch1);
        ItemsSketch<String> currentState = sketchAggregator._state();
        assertEquals(1L, currentState.getEstimate("1"));

        sketchAggregator._aggregate(sketch2);
        currentState = sketchAggregator._state();
        assertEquals(1L, currentState.getEstimate("1"));
        assertEquals(2L, currentState.getEstimate("3"));
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();
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
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();
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
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();
        sketchAggregator.init();
        sketchAggregator._aggregate(sketch1);
        final StringsSketchAggregator clone = sketchAggregator.statelessClone();
        assertNotSame(sketchAggregator, clone);
        clone._aggregate(sketch2);
        assertEquals(sketch2.getEstimate("1"), ((ItemsSketch<String>) clone.state()[0]).getEstimate("1"));
    }

    @Test
    public void testCloneWhenEmpty() {
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();
        sketchAggregator.init();
        final StringsSketchAggregator clone = sketchAggregator.statelessClone();
        assertNotSame(sketchAggregator, clone);
        clone._aggregate(sketch1);
        assertEquals(sketch1.getEstimate("1"), ((ItemsSketch<String>) clone.state()[0]).getEstimate("1"));
    }

    @Test
    public void testCloneOfBusySketch() {
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();
        sketchAggregator.init();
        for (int i = 0; i < 100; i++) {
            final ItemsSketch<String> union = new ItemsSketch<>(32);
            for (int j = 0; j < 100; j++) {
                union.update("" + j);
            }
            sketchAggregator._aggregate(union);
        }
        final StringsSketchAggregator clone = sketchAggregator.statelessClone();
        assertNotSame(sketchAggregator, clone);
        clone._aggregate(sketch1);
        assertEquals(sketch1.getEstimate("1"), ((ItemsSketch<String>) clone.state()[0]).getEstimate("1"));
    }

    @Test
    public void testEquals() {
        final ItemsSketch<String> sketch1 = new ItemsSketch<>(32);
        sketch1.update("1");
        final StringsSketchAggregator sketchAggregator1 = new StringsSketchAggregator();
        sketchAggregator1.aggregate(new ItemsSketch[]{sketch1});

        final ItemsSketch<String> sketch2 = new ItemsSketch<>(32);
        sketch2.update("1");
        final StringsSketchAggregator sketchAggregator2 = new StringsSketchAggregator();
        sketchAggregator2.aggregate(new ItemsSketch[]{sketch2});

        assertEquals(sketchAggregator1, sketchAggregator2);

        sketch2.update("2");
        sketchAggregator2.aggregate(new ItemsSketch[]{sketch2});
        assertNotEquals(sketchAggregator1, sketchAggregator2);
    }

    @Test
    public void testEqualsWhenEmpty() {
        assertEquals(new StringsSketchAggregator(), new StringsSketchAggregator());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsSketchAggregator aggregator = new StringsSketchAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.frequencies.function.aggregate.StringsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final StringsSketchAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), StringsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return StringsSketchAggregator.class;
    }

    @Override
    protected StringsSketchAggregator getInstance() {
        return new StringsSketchAggregator();
    }
}
