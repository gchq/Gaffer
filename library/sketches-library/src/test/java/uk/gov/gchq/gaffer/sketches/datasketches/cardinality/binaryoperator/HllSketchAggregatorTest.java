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
package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.binaryoperator;

import com.yahoo.sketches.hll.HllSketch;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HllSketchAggregatorTest extends BinaryOperatorTest {
    private static final double DELTA = 0.0000001D;
    private HllSketch sketch1;
    private HllSketch sketch2;

    @Before
    public void setup() {
        sketch1 = new HllSketch(10);
        sketch1.update("A");
        sketch1.update("B");

        sketch2 = new HllSketch(10);
        sketch2.update("C");
        sketch2.update("D");
    }

    @Test
    public void testAggregate() {
        final HllSketchAggregator sketchAggregator = new HllSketchAggregator();

        HllSketch currentState = sketch1;
        assertEquals(2.0D, currentState.getEstimate(), DELTA);

        currentState = sketchAggregator.apply(currentState, sketch2);
        assertEquals(4.0D, currentState.getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        assertEquals(new HllSketchAggregator(), new HllSketchAggregator());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final HllSketchAggregator aggregator = new HllSketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.binaryoperator.HllSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final HllSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), HllSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<HllSketchAggregator> getFunctionClass() {
        return HllSketchAggregator.class;
    }

    @Override
    protected HllSketchAggregator getInstance() {
        return new HllSketchAggregator();
    }
}
