/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator;

import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SketchAggregatorTest extends BinaryOperatorTest {

    private static final double DELTA = 0.01D;

    @Test
    public void testAggregate() {
        final SketchAggregator unionAggregator = new SketchAggregator();

        UpdateSketch sketch = UpdateSketch.builder().build();
        sketch.update("A");
        sketch.update("B");

        Sketch currentState = sketch;
        assertEquals(2.0D, currentState.getEstimate(), DELTA);

        UpdateSketch newSketch = UpdateSketch.builder().build();
        newSketch.update("C");
        newSketch.update("D");

        currentState = unionAggregator.apply(currentState, newSketch);
        assertEquals(4.0D, currentState.getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new SketchAggregator(), new SketchAggregator());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final SketchAggregator aggregator = new SketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.theta.binaryoperator.SketchAggregator\"%n" +
                "}"), json);

        // When 2
        final SketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), SketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return SketchAggregator.class;
    }

    @Override
    protected SketchAggregator getInstance() {
        return new SketchAggregator();
    }

    @Override
    protected Iterable<SketchAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
