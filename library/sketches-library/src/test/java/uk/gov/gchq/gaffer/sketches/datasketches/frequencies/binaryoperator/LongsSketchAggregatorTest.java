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

import com.yahoo.sketches.frequencies.LongsSketch;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class LongsSketchAggregatorTest extends BinaryOperatorTest {

    @Test
    public void testAggregate() {
        final LongsSketchAggregator sketchAggregator = new LongsSketchAggregator();

        LongsSketch currentSketch = new LongsSketch(32);
        currentSketch.update(1L);
        currentSketch.update(2L);
        currentSketch.update(3L);

        assertEquals(1L, currentSketch.getEstimate(1L));

        LongsSketch newSketch = new LongsSketch(32);
        newSketch.update(4L);
        newSketch.update(5L);
        newSketch.update(6L);
        newSketch.update(7L);
        newSketch.update(3L);

        currentSketch = sketchAggregator.apply(currentSketch, newSketch);
        assertEquals(1L, currentSketch.getEstimate(1L));
        assertEquals(2L, currentSketch.getEstimate(3L));
    }

    @Test
    public void testEquals() {
        assertEquals(new LongsSketchAggregator(), new LongsSketchAggregator());
    }

    @Test
    @Override
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

    @Override
    protected Iterable<LongsSketchAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
