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
package uk.gov.gchq.gaffer.sketches.datasketches.quantiles.function.aggregate;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesUnion;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class DoublesUnionAggregatorTest extends AggregateFunctionTest {
    private static final double DELTA = 0.01D;
    private DoublesUnion union1;
    private DoublesUnion union2;

    @Before
    public void setup() {
        union1 = DoublesUnion.builder().build();
        union1.update(1.0D);
        union1.update(2.0D);
        union1.update(3.0D);

        union2 = DoublesUnion.builder().build();
        union2.update(4.0D);
        union2.update(5.0D);
        union2.update(6.0D);
        union2.update(7.0D);
    }

    @Test
    public void testAggregate() {
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();
        unionAggregator.init();

        unionAggregator._aggregate(union1);
        DoublesSketch currentState = unionAggregator._state().getResult();
        assertEquals(3L, currentState.getN());
        assertEquals(2.0D, currentState.getQuantile(0.5D), DELTA);

        unionAggregator._aggregate(union2);
        currentState = unionAggregator._state().getResult();
        assertEquals(7L, currentState.getN());
        assertEquals(4.0D, currentState.getQuantile(0.5D), DELTA);
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();
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
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();
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
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        final DoublesUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union2);
        assertEquals(union2.getResult().getQuantile(0.5D), ((DoublesUnion) clone.state()[0]).getResult().getQuantile(0.5D), DELTA);
    }

    @Test
    public void testCloneWhenEmpty() {
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();
        unionAggregator.init();
        final DoublesUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getQuantile(0.5D), ((DoublesUnion) clone.state()[0]).getResult().getQuantile(0.5D), DELTA);
    }

    @Test
    public void testCloneOfBusySketch() {
        final DoublesUnionAggregator unionAggregator = new DoublesUnionAggregator();
        unionAggregator.init();
        for (int i = 0; i < 100; i++) {
            final DoublesUnion union = DoublesUnion.builder().build();
            for (int j = 0; j < 100; j++) {
                union.update(Math.random());
            }
            unionAggregator._aggregate(union);
        }
        final DoublesUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getQuantile(0.5D), ((DoublesUnion) clone.state()[0]).getResult().getQuantile(0.5D), DELTA);
    }

    @Test
    public void testEquals() {
        final DoublesUnion sketch1 = DoublesUnion.builder().build();
        sketch1.update(1.0D);
        final DoublesUnionAggregator sketchAggregator1 = new DoublesUnionAggregator();
        sketchAggregator1.aggregate(new DoublesUnion[]{sketch1});

        final DoublesUnion sketch2 = DoublesUnion.builder().build();
        sketch2.update(1.0D);
        final DoublesUnionAggregator sketchAggregator2 = new DoublesUnionAggregator();
        sketchAggregator2.aggregate(new DoublesUnion[]{sketch2});

        assertEquals(sketchAggregator1, sketchAggregator2);

        sketch2.update(2.0D);
        sketchAggregator2.aggregate(new DoublesUnion[]{sketch2});
        assertNotEquals(sketchAggregator1, sketchAggregator2);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final DoublesUnionAggregator aggregator = new DoublesUnionAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.function.aggregate.DoublesUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final DoublesUnionAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), DoublesUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return DoublesUnionAggregator.class;
    }

    @Override
    protected DoublesUnionAggregator getInstance() {
        return new DoublesUnionAggregator();
    }
}
