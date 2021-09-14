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
package uk.gov.gchq.gaffer.sketches.datasketches.frequencies.binaryoperator;

import com.yahoo.sketches.frequencies.ItemsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StringsSketchAggregatorTest extends BinaryOperatorTest {

    @Test
    public void testAggregate() {
        final StringsSketchAggregator sketchAggregator = new StringsSketchAggregator();

        ItemsSketch<String> currentSketch = new ItemsSketch<>(32);
        currentSketch.update("1");
        currentSketch.update("2");
        currentSketch.update("3");

        assertEquals(1L, currentSketch.getEstimate("1"));

        ItemsSketch<String> newSketch = new ItemsSketch<>(32);
        newSketch.update("4");
        newSketch.update("5");
        newSketch.update("6");
        newSketch.update("7");
        newSketch.update("3");

        currentSketch = sketchAggregator.apply(currentSketch, newSketch);
        assertEquals(1L, currentSketch.getEstimate("1"));
        assertEquals(2L, currentSketch.getEstimate("3"));
    }

    @Test
    public void testEquals() {
        assertEquals(new StringsSketchAggregator(), new StringsSketchAggregator());
    }

    @Test
    @Override
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

    @Override
    protected Iterable<StringsSketchAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
