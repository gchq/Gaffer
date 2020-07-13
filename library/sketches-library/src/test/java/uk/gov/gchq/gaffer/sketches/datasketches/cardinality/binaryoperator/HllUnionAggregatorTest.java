/*
 * Copyright 2017-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.binaryoperator;

import com.yahoo.sketches.hll.Union;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HllUnionAggregatorTest extends BinaryOperatorTest {

    private static final double DELTA = 0.0000001D;

    @Test
    public void testAggregate() {
        final HllUnionAggregator sketchAggregator = new HllUnionAggregator();

        Union currentSketch = new Union(15);
        currentSketch.update("A");
        currentSketch.update("B");

        assertEquals(2.0D, currentSketch.getEstimate(), DELTA);

        Union newSketch = new Union(15);
        newSketch.update("C");
        newSketch.update("D");

        currentSketch = sketchAggregator.apply(currentSketch, newSketch);
        assertEquals(4.0D, currentSketch.getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new HllUnionAggregator(), new HllUnionAggregator());
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final HllUnionAggregator aggregator = new HllUnionAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.binaryoperator.HllUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final HllUnionAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), HllUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<HllUnionAggregator> getFunctionClass() {
        return HllUnionAggregator.class;
    }

    @Override
    protected HllUnionAggregator getInstance() {
        return new HllUnionAggregator();
    }
}
