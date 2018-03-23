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

import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReservoirLongUnionAggregatorTest extends BinaryOperatorTest {
    private ReservoirLongsUnion union1;
    private ReservoirLongsUnion union2;

    @Before
    public void setup() {
        union1 = ReservoirLongsUnion.newInstance(20);
        union1.update(1L);
        union1.update(2L);
        union1.update(3L);

        union2 = ReservoirLongsUnion.newInstance(20);
        for (long l = 4L; l < 100; l++) {
            union2.update(l);
        }
    }

    @Test
    public void testAggregate() {
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();

        ReservoirLongsUnion currentState = union1;
        assertEquals(3L, currentState.getResult().getN());
        assertEquals(3, currentState.getResult().getNumSamples());
        // As less items have been added than the capacity, the sample should exactly match what was added.
        Set<Long> samples = new HashSet<>(Arrays.asList(ArrayUtils.toObject(currentState.getResult().getSamples())));
        Set<Long> expectedSamples = new HashSet<>();
        expectedSamples.add(1L);
        expectedSamples.add(2L);
        expectedSamples.add(3L);
        assertEquals(expectedSamples, samples);

        currentState = unionAggregator.apply(currentState, union2);
        assertEquals(99L, currentState.getResult().getN());
        assertEquals(20L, currentState.getResult().getNumSamples());
        // As more items have been added than the capacity, we can't know exactly what items will be present
        // in the sample but we can check that they are all from the set of things we added.
        samples = new HashSet<>(Arrays.asList(ArrayUtils.toObject(currentState.getResult().getSamples())));
        for (long l = 4L; l < 100; l++) {
            expectedSamples.add(l);
        }
        assertTrue(expectedSamples.containsAll(samples));
    }

    @Test
    public void testEquals() {
        assertEquals(new ReservoirLongsUnionAggregator(), new ReservoirLongsUnionAggregator());
    }

    @Override
    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ReservoirLongsUnionAggregator aggregator = new ReservoirLongsUnionAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));
        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.sampling.binaryoperator.ReservoirLongsUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final ReservoirLongsUnionAggregator deserialisedAggregator = JSONSerialiser
                .deserialise(json.getBytes(), ReservoirLongsUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends BinaryOperator> getFunctionClass() {
        return ReservoirLongsUnionAggregator.class;
    }

    @Override
    protected ReservoirLongsUnionAggregator getInstance() {
        return new ReservoirLongsUnionAggregator();
    }
}
