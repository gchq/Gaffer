/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.sampling.function.aggregate;

import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ReservoirItemsUnionAggregatorTest extends AggregateFunctionTest {
    private static final Random RANDOM = new Random();
    private ReservoirItemsUnion<String> union1;
    private ReservoirItemsUnion<String> union2;

    @Before
    public void setup() {
        union1 = ReservoirItemsUnion.getInstance(20);
        union1.update("1");
        union1.update("2");
        union1.update("3");

        union2 = ReservoirItemsUnion.getInstance(20);
        for (int i = 4; i < 100; i++) {
            union2.update("" + i);
        }
    }

    @Test
    public void testAggregate() {
        final ReservoirItemsUnionAggregator<String> unionAggregator = new ReservoirItemsUnionAggregator<>();
        unionAggregator.init();

        unionAggregator._aggregate(union1);
        ReservoirItemsSketch<String> currentState = unionAggregator._state().getResult();
        assertEquals(3L, currentState.getN());
        assertEquals(3, currentState.getNumSamples());
        // As less items have been added than the capacity, the sample should exactly match what was added.
        Set<String> samples = new HashSet<>(Arrays.asList(currentState.getSamples()));
        Set<String> expectedSamples = new HashSet<>();
        expectedSamples.add("1");
        expectedSamples.add("2");
        expectedSamples.add("3");
        assertEquals(expectedSamples, samples);

        unionAggregator._aggregate(union2);
        currentState = unionAggregator._state().getResult();
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
    public void testFailedExecuteDueToNullInput() {
        final ReservoirItemsUnionAggregator<String> unionAggregator = new ReservoirItemsUnionAggregator<>();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        try {
            unionAggregator.aggregate(null);
        } catch (final IllegalArgumentException exception) {
            assertEquals("Expected an input array of length 1", exception.getMessage());
        }
    }

    @Test
    public void testFailedExecuteDueToEmptyInput() {
        final ReservoirItemsUnionAggregator<String> unionAggregator = new ReservoirItemsUnionAggregator<>();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        try {
            unionAggregator.aggregate(new Object[0]);
        } catch (final IllegalArgumentException exception) {
            assertEquals("Expected an input array of length 1", exception.getMessage());
        }
    }

    @Test
    public void testClone() {
        final ReservoirItemsUnionAggregator<String> unionAggregator = new ReservoirItemsUnionAggregator<>();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        final ReservoirItemsUnionAggregator<String> clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union2);
        assertEquals(union2.getResult().getN(), ((ReservoirItemsUnion) clone.state()[0]).getResult().getN());
    }

    @Test
    public void testCloneWhenEmpty() {
        final ReservoirItemsUnionAggregator<String> unionAggregator = new ReservoirItemsUnionAggregator<>();
        unionAggregator.init();
        final ReservoirItemsUnionAggregator<String> clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getN(), ((ReservoirItemsUnion) clone.state()[0]).getResult().getN());
    }

    @Test
    public void testCloneOfBusySketch() {
        final ReservoirItemsUnionAggregator<String> unionAggregator = new ReservoirItemsUnionAggregator<>();
        unionAggregator.init();
        for (int i = 0; i < 100; i++) {
            final ReservoirItemsUnion<String> union = ReservoirItemsUnion.getInstance(20);
            for (int j = 0; j < 100; j++) {
                union.update("" + RANDOM.nextLong());
            }
            unionAggregator._aggregate(union);
        }
        final ReservoirItemsUnionAggregator<String> clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getN(), ((ReservoirItemsUnion) clone.state()[0]).getResult().getN());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ReservoirItemsUnionAggregator aggregator = new ReservoirItemsUnionAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.sampling.function.aggregate.ReservoirItemsUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final ReservoirItemsUnionAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), ReservoirItemsUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return ReservoirItemsUnionAggregator.class;
    }

    @Override
    protected ReservoirItemsUnionAggregator getInstance() {
        return new ReservoirItemsUnionAggregator();
    }
}
