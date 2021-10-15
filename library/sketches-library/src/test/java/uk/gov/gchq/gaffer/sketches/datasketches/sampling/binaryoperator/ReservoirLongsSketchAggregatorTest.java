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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.binaryoperator;

import com.yahoo.sketches.sampling.ReservoirLongsSketch;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReservoirLongsSketchAggregatorTest extends BinaryOperatorTest {

    @Test
    public void testAggregate() {
        final ReservoirLongsSketchAggregator sketchAggregator = new ReservoirLongsSketchAggregator();

        ReservoirLongsSketch currentSketch = ReservoirLongsSketch.newInstance(20);
        currentSketch.update(1L);
        currentSketch.update(2L);
        currentSketch.update(3L);

        assertEquals(3L, currentSketch.getN());
        assertEquals(3, currentSketch.getNumSamples());

        // As less items have been added than the capacity, the sample should exactly match what was added.
        Set<Long> samples = new HashSet<>(Arrays.asList(ArrayUtils.toObject(currentSketch.getSamples())));
        Set<Long> expectedSamples = new HashSet<>();
        expectedSamples.add(1L);
        expectedSamples.add(2L);
        expectedSamples.add(3L);
        assertEquals(expectedSamples, samples);

        ReservoirLongsSketch newSketch = ReservoirLongsSketch.newInstance(20);
        for (long l = 4L; l < 100; l++) {
            newSketch.update(l);
        }

        currentSketch = sketchAggregator.apply(currentSketch, newSketch);
        assertEquals(99L, currentSketch.getN());
        assertEquals(20L, currentSketch.getNumSamples());
        // As more items have been added than the capacity, we can't know exactly what items will be present
        // in the sample but we can check that they are all from the set of things we added.
        samples = new HashSet<>(Arrays.asList(ArrayUtils.toObject(currentSketch.getSamples())));
        for (long l = 4L; l < 100; l++) {
            expectedSamples.add(l);
        }
        assertTrue(expectedSamples.containsAll(samples));
    }

    @Test
    public void testEquals() {
        assertEquals(new ReservoirLongsSketchAggregator(), new ReservoirLongsSketchAggregator());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ReservoirLongsSketchAggregator aggregator = new ReservoirLongsSketchAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.sampling.binaryoperator.ReservoirLongsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final ReservoirLongsSketchAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), ReservoirLongsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return ReservoirLongsSketchAggregator.class;
    }

    @Override
    protected ReservoirLongsSketchAggregator getInstance() {
        return new ReservoirLongsSketchAggregator();
    }

    @Override
    protected Iterable<ReservoirLongsSketchAggregator> getDifferentInstancesOrNull() {
        return null;
    }
}
