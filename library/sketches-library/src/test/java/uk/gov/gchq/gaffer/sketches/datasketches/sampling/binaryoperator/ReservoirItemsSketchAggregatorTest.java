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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.binaryoperator;

import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReservoirItemsSketchAggregatorTest extends BinaryOperatorTest {
    private static final Random RANDOM = new Random();
    private ReservoirItemsSketch<String> union1;
    private ReservoirItemsSketch<String> union2;

    @Before
    public void setup() {
        union1 = ReservoirItemsSketch.newInstance(20);
        union1.update("1");
        union1.update("2");
        union1.update("3");

        union2 = ReservoirItemsSketch.newInstance(20);
        for (int i = 4; i < 100; i++) {
            union2.update("" + i);
        }
    }

    @Test
    public void testAggregate() {
        final ReservoirItemsSketchAggregator<String> sketchAggregator = new ReservoirItemsSketchAggregator<>();

        ReservoirItemsSketch<String> currentState = union1;
        assertEquals(3L, currentState.getN());
        assertEquals(3, currentState.getNumSamples());
        // As less items have been added than the capacity, the sample should exactly match what was added.
        Set<String> samples = new HashSet<>(Arrays.asList(currentState.getSamples()));
        Set<String> expectedSamples = new HashSet<>();
        expectedSamples.add("1");
        expectedSamples.add("2");
        expectedSamples.add("3");
        assertEquals(expectedSamples, samples);

        currentState = sketchAggregator.apply(currentState, union2);
        assertEquals(99L, currentState.getN());
        assertEquals(20L, currentState.getNumSamples());
        // As more items have been added than the capacity, we can't know exactly what items will be present
        // in the sample but we can check that they are all from the set of things we added.
        samples = new HashSet<>(Arrays.asList(currentState.getSamples()));
        for (long i = 4L; i < 100; i++) {
            expectedSamples.add("" + i);
        }
        assertTrue(expectedSamples.containsAll(samples));
    }

    @Test
    public void testEquals() {
        assertEquals(new ReservoirItemsSketchAggregator<String>(), new ReservoirItemsSketchAggregator<String>());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ReservoirItemsSketchAggregator<String> aggregator = new ReservoirItemsSketchAggregator<>();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.sampling.binaryoperator.ReservoirItemsSketchAggregator\"%n" +
                "}"), json);

        // When 2
        final ReservoirItemsSketchAggregator<String> deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), ReservoirItemsSketchAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return ReservoirItemsSketchAggregator.class;
    }

    @Override
    protected ReservoirItemsSketchAggregator getInstance() {
        return new ReservoirItemsSketchAggregator();
    }
}
