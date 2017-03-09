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
package uk.gov.gchq.gaffer.sketches.datasketches.theta.function.aggregate;

import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
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

public class UnionAggregatorTest extends AggregateFunctionTest {
    private static final double DELTA = 0.01D;
    private Union union1;
    private Union union2;

    @Before
    public void setup() {
        union1 = Sketches.setOperationBuilder().buildUnion();
        union1.update("A");
        union1.update("B");

        union2 = Sketches.setOperationBuilder().buildUnion();
        union2.update("C");
        union2.update("D");
    }

    @Test
    public void testAggregate() {
        final UnionAggregator unionAggregator = new UnionAggregator();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        assertEquals(2.0D, unionAggregator._state().getResult().getEstimate(), DELTA);
        unionAggregator._aggregate(union2);
        assertEquals(4.0D, unionAggregator._state().getResult().getEstimate(), DELTA);
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        final UnionAggregator unionAggregator = new UnionAggregator();
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
        final UnionAggregator unionAggregator = new UnionAggregator();
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
        final UnionAggregator unionAggregator = new UnionAggregator();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        final UnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union2);
        assertEquals(union2.getResult().getEstimate(), ((Union) clone.state()[0]).getResult().getEstimate(), DELTA);
    }

    @Test
    public void testCloneWhenEmpty() {
        final UnionAggregator unionAggregator = new UnionAggregator();
        unionAggregator.init();
        final UnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getEstimate(), ((Union) clone.state()[0]).getResult().getEstimate(), DELTA);
    }

    @Test
    public void testCloneOfBusySketch() {
        final UnionAggregator unionAggregator = new UnionAggregator();
        unionAggregator.init();
        for (int i = 0; i < 100; i++) {
            final Union union = SetOperation.builder().buildUnion();
            for (int j = 0; j < 100; j++) {
                union.update(Math.random());
            }
            unionAggregator._aggregate(union);
        }
        final UnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getEstimate(), ((Union) clone.state()[0]).getResult().getEstimate(), DELTA);
    }

    @Test
    public void testEquals() {
        final Union sketch1 = Sketches.setOperationBuilder().buildUnion();
        sketch1.update("A");
        final UnionAggregator sketchAggregator1 = new UnionAggregator();
        sketchAggregator1.aggregate(new Union[]{sketch1});

        final Union sketch2 = Sketches.setOperationBuilder().buildUnion();
        sketch2.update("A");
        final UnionAggregator sketchAggregator2 = new UnionAggregator();
        sketchAggregator2.aggregate(new Union[]{sketch2});

        assertEquals(sketchAggregator1, sketchAggregator2);

        sketch2.update("B");
        sketchAggregator2.aggregate(new Union[]{sketch2});
        assertNotEquals(sketchAggregator1, sketchAggregator2);
    }

    @Test
    public void testEqualsWhenEmpty() {
        assertEquals(new UnionAggregator(), new UnionAggregator());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final UnionAggregator aggregator = new UnionAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.theta.function.aggregate.UnionAggregator\"%n" +
                "}"), json);

        // When 2
        final UnionAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), UnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return UnionAggregator.class;
    }

    @Override
    protected UnionAggregator getInstance() {
        return new UnionAggregator();
    }
}
