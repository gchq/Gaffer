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

import com.yahoo.sketches.sampling.ReservoirLongsSketch;
import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import org.apache.commons.lang.ArrayUtils;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ReservoirLongUnionAggregatorTest extends AggregateFunctionTest {
    private static final Random RANDOM = new Random();
    private ReservoirLongsUnion union1;
    private ReservoirLongsUnion union2;

    @Before
    public void setup() {
        union1 = ReservoirLongsUnion.getInstance(20);
        union1.update(1L);
        union1.update(2L);
        union1.update(3L);

        union2 = ReservoirLongsUnion.getInstance(20);
        for (long l = 4L; l < 100; l++) {
            union2.update(l);
        }
    }

    @Test
    public void testAggregate() {
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();
        unionAggregator.init();

        unionAggregator._aggregate(union1);
        ReservoirLongsSketch currentState = unionAggregator._state().getResult();
        assertEquals(3L, currentState.getN());
        assertEquals(3, currentState.getNumSamples());
        // As less items have been added than the capacity, the sample should exactly match what was added.
        Set<Long> samples = new HashSet<>(Arrays.asList(ArrayUtils.toObject(currentState.getSamples())));
        Set<Long> expectedSamples = new HashSet<>();
        expectedSamples.add(1L);
        expectedSamples.add(2L);
        expectedSamples.add(3L);
        assertEquals(expectedSamples, samples);

        unionAggregator._aggregate(union2);
        currentState = unionAggregator._state().getResult();
        assertEquals(99L, currentState.getN());
        assertEquals(20L, currentState.getNumSamples());
        // As more items have been added than the capacity, we can't know exactly what items will be present
        // in the sample but we can check that they are all from the set of things we added.
        samples = new HashSet<>(Arrays.asList(ArrayUtils.toObject(currentState.getSamples())));
        for (long l = 4L; l < 100; l++) {
            expectedSamples.add(l);
        }
        assertTrue(expectedSamples.containsAll(samples));
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();
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
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();
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
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        final ReservoirLongsUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union2);
        assertEquals(union2.getResult().getN(), ((ReservoirLongsUnion) clone.state()[0]).getResult().getN());
    }

    @Test
    public void testCloneWhenEmpty() {
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();
        unionAggregator.init();
        final ReservoirLongsUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getN(), ((ReservoirLongsUnion) clone.state()[0]).getResult().getN());
    }

    @Test
    public void testCloneOfBusySketch() {
        final ReservoirLongsUnionAggregator unionAggregator = new ReservoirLongsUnionAggregator();
        unionAggregator.init();
        for (int i = 0; i < 100; i++) {
            final ReservoirLongsUnion union = ReservoirLongsUnion.getInstance(20);
            for (int j = 0; j < 100; j++) {
                union.update(RANDOM.nextLong());
            }
            unionAggregator._aggregate(union);
        }
        final ReservoirLongsUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getN(), ((ReservoirLongsUnion) clone.state()[0]).getResult().getN());
    }

    @Test
    public void testEquals() {
        final ReservoirLongsUnion sketch1 = ReservoirLongsUnion.getInstance(20);
        sketch1.update(1L);
        final ReservoirLongsUnionAggregator sketchAggregator1 = new ReservoirLongsUnionAggregator();
        sketchAggregator1.aggregate(new ReservoirLongsUnion[]{sketch1});

        final ReservoirLongsUnion sketch2 = ReservoirLongsUnion.getInstance(20);
        sketch2.update(1L);
        final ReservoirLongsUnionAggregator sketchAggregator2 = new ReservoirLongsUnionAggregator();
        sketchAggregator2.aggregate(new ReservoirLongsUnion[]{sketch2});

        assertEquals(sketchAggregator1, sketchAggregator2);

        sketch2.update(2L);
        sketchAggregator2.aggregate(new ReservoirLongsUnion[]{sketch2});
        assertNotEquals(sketchAggregator1, sketchAggregator2);
    }

    @Test
    public void testEqualsWhenEmpty() {
        assertEquals(new ReservoirLongsUnionAggregator(), new ReservoirLongsUnionAggregator());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ReservoirLongsUnionAggregator aggregator = new ReservoirLongsUnionAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.sampling.function.aggregate.ReservoirLongsUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final ReservoirLongsUnionAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), ReservoirLongsUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return ReservoirLongsUnionAggregator.class;
    }

    @Override
    protected ReservoirLongsUnionAggregator getInstance() {
        return new ReservoirLongsUnionAggregator();
    }
}
