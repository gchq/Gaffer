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

import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.Comparator;
import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StringsSketchAggregatorTest extends BinaryOperatorTest<StringsSketchAggregator> {

    @Test
    public void testAggregate() {
        final StringsSketchAggregator unionAggregator = new StringsSketchAggregator();

        ItemsSketch<String> currentSketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
        currentSketch.update("1");
        currentSketch.update("2");
        currentSketch.update("3");

        assertEquals(3L, currentSketch.getN());
        assertEquals("2", currentSketch.getQuantile(0.5D, QuantileSearchCriteria.EXCLUSIVE));

        ItemsSketch<String> newSketch = ItemsSketch.getInstance(String.class, Comparator.naturalOrder());
        newSketch.update("4");
        newSketch.update("5");
        newSketch.update("6");
        newSketch.update("7");

        currentSketch = unionAggregator.apply(currentSketch, newSketch);
        assertEquals(7L, currentSketch.getN());
        assertEquals("4", currentSketch.getQuantile(0.5D, QuantileSearchCriteria.EXCLUSIVE));
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
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.binaryoperator.StringsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final StringsSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), StringsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
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
