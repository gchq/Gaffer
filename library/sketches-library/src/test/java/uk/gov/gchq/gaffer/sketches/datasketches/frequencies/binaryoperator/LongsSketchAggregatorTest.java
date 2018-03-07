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

import com.yahoo.sketches.frequencies.LongsSketch;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LongsSketchAggregatorTest extends BinaryOperatorTest {
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

        LongsSketch currentState = sketch1;
        assertEquals(1L, currentState.getEstimate(1L));

        currentState = sketchAggregator.apply(currentState, sketch2);
        assertEquals(1L, currentState.getEstimate(1L));
        assertEquals(2L, currentState.getEstimate(3L));
    }

    @Test
    public void testEquals() {
        assertEquals(new LongsSketchAggregator(), new LongsSketchAggregator());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final LongsSketchAggregator aggregator = new LongsSketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.frequencies.binaryoperator.LongsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final LongsSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), LongsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<LongsSketchAggregator> getFunctionClass() {
        return LongsSketchAggregator.class;
    }

    @Override
    protected LongsSketchAggregator getInstance() {
        return new LongsSketchAggregator();
    }
}
