/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DoublesSketchAggregatorTest extends BinaryOperatorTest<DoublesSketchAggregator> {

    private static final double DELTA = 0.01D;

    @Test
    public void testAggregate() {
        final DoublesSketchAggregator sketchAggregator = new DoublesSketchAggregator();

        UpdateDoublesSketch sketch1 = DoublesSketch.builder().build();
        sketch1.update(1.0D);
        sketch1.update(2.0D);
        sketch1.update(3.0D);

        DoublesSketch currentState = sketch1;
        assertEquals(3L, currentState.getN());
        assertEquals(2.0D, currentState.getQuantile(0.5D), DELTA);

        UpdateDoublesSketch sketch2 = DoublesSketch.builder().build();
        sketch2.update(4.0D);
        sketch2.update(5.0D);
        sketch2.update(6.0D);
        sketch2.update(7.0D);

        currentState = sketchAggregator.apply(currentState, sketch2);
        assertEquals(7L, currentState.getN());
        assertEquals(4.0D, currentState.getQuantile(0.5D), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new DoublesSketchAggregator(), new DoublesSketchAggregator());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final DoublesSketchAggregator aggregator = new DoublesSketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator.DoublesSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final DoublesSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), DoublesSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return DoublesSketchAggregator.class;
    }

    @Override
    protected DoublesSketchAggregator getInstance() {
        return new DoublesSketchAggregator();
    }

    @Override
    protected Iterable<DoublesSketchAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
