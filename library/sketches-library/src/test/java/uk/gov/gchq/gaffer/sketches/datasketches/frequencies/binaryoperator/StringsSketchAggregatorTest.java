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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.binaryoperator;

import com.yahoo.sketches.frequencies.ItemsSketch;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringsSketchAggregatorTest extends BinaryOperatorTest {
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

        ItemsSketch<String> currentState = sketch1;
        assertEquals(1L, currentState.getEstimate("1"));

        currentState = sketchAggregator.apply(currentState, sketch2);
        assertEquals(1L, currentState.getEstimate("1"));
        assertEquals(2L, currentState.getEstimate("3"));
    }

    @Test
    public void testEquals() {
        assertEquals(new StringsSketchAggregator(), new StringsSketchAggregator());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsSketchAggregator aggregator = new StringsSketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.frequencies.binaryoperator.StringsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final StringsSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), StringsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator
            > getFunctionClass() {
        return StringsSketchAggregator.class;
    }

    @Override
    protected StringsSketchAggregator getInstance() {
        return new StringsSketchAggregator();
    }
}
