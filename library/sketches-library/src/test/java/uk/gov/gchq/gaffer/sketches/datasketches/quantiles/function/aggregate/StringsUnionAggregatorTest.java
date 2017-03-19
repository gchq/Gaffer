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

import com.google.common.collect.Ordering;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
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

public class StringsUnionAggregatorTest extends AggregateFunctionTest {
    private ItemsUnion<String> union1;
    private ItemsUnion<String> union2;

    @Before
    public void setup() {
        union1 = ItemsUnion.getInstance(Ordering.<String>natural());
        union1.update("1");
        union1.update("2");
        union1.update("3");

        union2 = ItemsUnion.getInstance(Ordering.<String>natural());
        union2.update("4");
        union2.update("5");
        union2.update("6");
        union2.update("7");
    }

    @Test
    public void testAggregate() {
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
        unionAggregator.init();

        unionAggregator._aggregate(union1);
        ItemsSketch<String> currentState = unionAggregator._state().getResult();
        assertEquals(3L, currentState.getN());
        assertEquals("2", currentState.getQuantile(0.5D));

        unionAggregator._aggregate(union2);
        currentState = unionAggregator._state().getResult();
        assertEquals(7L, currentState.getN());
        assertEquals("4", currentState.getQuantile(0.5D));
    }

    @Test
    public void testFailedExecuteDueToNullInput() {
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
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
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
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
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
        unionAggregator.init();
        unionAggregator._aggregate(union1);
        final StringsUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union2);
        assertEquals(union2.getResult().getQuantile(0.5D), ((ItemsUnion<String>) clone.state()[0]).getResult().getQuantile(0.5D));
    }

    @Test
    public void testCloneWhenEmpty() {
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
        unionAggregator.init();
        final StringsUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getQuantile(0.5D), ((ItemsUnion<String>) clone.state()[0]).getResult().getQuantile(0.5D));
    }

    @Test
    public void testCloneOfBusySketch() {
        final StringsUnionAggregator unionAggregator = new StringsUnionAggregator();
        unionAggregator.init();
        for (int i = 0; i < 100; i++) {
            final ItemsUnion<String> union = ItemsUnion.getInstance(Ordering.<String>natural());
            for (int j = 0; j < 100; j++) {
                union.update("" + Math.random());
            }
            unionAggregator._aggregate(union);
        }
        final StringsUnionAggregator clone = unionAggregator.statelessClone();
        assertNotSame(unionAggregator, clone);
        clone._aggregate(union1);
        assertEquals(union1.getResult().getQuantile(0.5D), ((ItemsUnion<String>) clone.state()[0]).getResult().getQuantile(0.5D));
    }

    @Test
    public void testEquals() {
        final ItemsUnion<String> sketch1 = ItemsUnion.getInstance(Ordering.<String>natural());
        sketch1.update("1");
        final StringsUnionAggregator sketchAggregator1 = new StringsUnionAggregator();
        sketchAggregator1.aggregate(new ItemsUnion[]{sketch1});

        final ItemsUnion<String> sketch2 = ItemsUnion.getInstance(Ordering.<String>natural());
        sketch2.update("1");
        final StringsUnionAggregator sketchAggregator2 = new StringsUnionAggregator();
        sketchAggregator2.aggregate(new ItemsUnion[]{sketch2});

        assertEquals(sketchAggregator1, sketchAggregator2);

        sketch2.update("2");
        sketchAggregator2.aggregate(new ItemsUnion[]{sketch2});
        assertNotEquals(sketchAggregator1, sketchAggregator2);
    }

    @Test
    public void testEqualsWhenEmpty() {
        assertEquals(new StringsUnionAggregator(), new StringsUnionAggregator());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsUnionAggregator aggregator = new StringsUnionAggregator();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));
        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.sketches.datasketches.quantiles.function.aggregate.StringsUnionAggregator\"%n" +
                "}"), json);

        // When 2
        final StringsUnionAggregator deserialisedAggregator = new JSONSerialiser()
                .deserialise(json.getBytes(), StringsUnionAggregator.class);
        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return StringsUnionAggregator.class;
    }

    @Override
    protected StringsUnionAggregator getInstance() {
        return new StringsUnionAggregator();
    }
}
