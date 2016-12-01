/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.function.aggregate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.AggregateFunctionTest;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MaxTest extends AggregateFunctionTest {

    @Test
    public void testInitialise() {
        // Given 1
        final Max intMax = new Max();

        // When 1
        intMax.init();

        // Then
        assertNull(intMax.state()[0]);
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Max intMax = new Max();
        intMax.setMode(NumericAggregateFunction.NumberType.INT);

        intMax.init();

        // When 1
        intMax._aggregate(1);

        // Then 1
        assertTrue(intMax.state()[0] instanceof Integer);
        assertEquals(1, intMax.state()[0]);

        // When 2
        intMax._aggregate(3);

        // Then 2
        assertTrue(intMax.state()[0] instanceof Integer);
        assertEquals(3, intMax.state()[0]);

        // When 3
        intMax._aggregate(2);

        // Then 3
        assertTrue(intMax.state()[0] instanceof Integer);
        assertEquals(3, intMax.state()[0]);
    }

    @Test
    public void testAggregateInIntModeMixedInput() {
        // Given
        final Max intMax = new Max();
        intMax.setMode(NumericAggregateFunction.NumberType.INT);

        intMax.init();

        // When 1
        intMax._aggregate(1);

        // Then 1
        assertTrue(intMax.state()[0] instanceof Integer);
        assertEquals(1, intMax.state()[0]);

        // When 2
        try {
            intMax._aggregate(2.7d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(intMax.state()[0] instanceof Integer);
        assertEquals(1, intMax.state()[0]);

        // When 3
        try {
            intMax._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(intMax.state()[0] instanceof Integer);
        assertEquals(1, intMax.state()[0]);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Max longMax = new Max();
        longMax.setMode(NumericAggregateFunction.NumberType.LONG);

        longMax.init();

        // When 1
        longMax._aggregate(2l);

        // Then 1
        assertTrue(longMax.state()[0] instanceof Long);
        assertEquals(2l, longMax.state()[0]);

        // When 2
        longMax._aggregate(1l);

        // Then 2
        assertTrue(longMax.state()[0] instanceof Long);
        assertEquals(2l, longMax.state()[0]);

        // When 3
        longMax._aggregate(3l);

        // Then 3
        assertTrue(longMax.state()[0] instanceof Long);
        assertEquals(3l, longMax.state()[0]);
    }

    @Test
    public void testAggregateInLongModeMixedInput() {
        // Given
        final Max longMax = new Max();
        longMax.setMode(NumericAggregateFunction.NumberType.LONG);

        longMax.init();

        // When 1
        try {
            longMax._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(longMax.state()[0]);

        // When 2
        longMax._aggregate(3l);

        // Then 2
        assertTrue(longMax.state()[0] instanceof Long);
        assertEquals(3l, longMax.state()[0]);

        // When 3
        try {
            longMax._aggregate(2.5d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(longMax.state()[0] instanceof Long);
        assertEquals(3l, longMax.state()[0]);
    }

    @Test
    public void testAggregateInDoubleMode() {
        // Given
        final Max doubleMax = new Max();
        doubleMax.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleMax.init();

        // When 1
        doubleMax._aggregate(1.1d);

        // Then 1
        assertTrue(doubleMax.state()[0] instanceof Double);
        assertEquals(1.1d, doubleMax.state()[0]);

        // When 2
        doubleMax._aggregate(2.1d);

        // Then 2
        assertTrue(doubleMax.state()[0] instanceof Double);
        assertEquals(2.1d, doubleMax.state()[0]);

        // When 3
        doubleMax._aggregate(1.5d);

        // Then 3
        assertTrue(doubleMax.state()[0] instanceof Double);
        assertEquals(2.1d, doubleMax.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeMixedInput() {
        // Given
        final Max doubleMax = new Max();
        doubleMax.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleMax.init();

        // When 1
        try {
            doubleMax._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(doubleMax.state()[0]);

        // When 2
        try {
            doubleMax._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertNull(doubleMax.state()[0]);

        // When 3
        doubleMax._aggregate(2.1d);

        // Then 3
        assertTrue(doubleMax.state()[0] instanceof Double);
        assertEquals(2.1d, doubleMax.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirst() {
        // Given
        final Max max = new Max();

        max.init();

        // When 1
        max._aggregate(1);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.INT, max.getMode());
        assertTrue(max.state()[0] instanceof Integer);
        assertEquals(1, max.state()[0]);

        // When 2
        try {
            max._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.INT, max.getMode());
        assertTrue(max.state()[0] instanceof Integer);
        assertEquals(1, max.state()[0]);

        // When 3
        try {
            max._aggregate(2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.INT, max.getMode());
        assertTrue(max.state()[0] instanceof Integer);
        assertEquals(1, max.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirst() {
        // Given
        final Max max = new Max();

        max.init();

        // When 1
        max._aggregate(1l);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.LONG, max.getMode());
        assertTrue(max.state()[0] instanceof Long);
        assertEquals(1l, max.state()[0]);

        // When 2
        try {
            max._aggregate(3);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.LONG, max.getMode());
        assertTrue(max.state()[0] instanceof Long);
        assertEquals(1l, max.state()[0]);

        // When 3
        try {
            max._aggregate(2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.LONG, max.getMode());
        assertTrue(max.state()[0] instanceof Long);
        assertEquals(1l, max.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirst() {
        // Given
        final Max max = new Max();

        max.init();

        // When 1
        max._aggregate(1.1d);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, max.getMode());
        assertTrue(max.state()[0] instanceof Double);
        assertEquals(1.1d, max.state()[0]);

        // When 2
        try {
            max._aggregate(2);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, max.getMode());
        assertTrue(max.state()[0] instanceof Double);
        assertEquals(1.1d, max.state()[0]);

        // When 3
        try {
            max._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, max.getMode());
        assertTrue(max.state()[0] instanceof Double);
        assertEquals(1.1d, max.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeNullFirst() {
        // Given
        final Max max = new Max();
        max.init();

        // When 1
        max.aggregate(new Object[]{null});
        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.AUTO, max.getMode());
        assertEquals(null, max.state()[0]);
    }

    @Test
    public void testAggregateNullFirst() {
        // Given
        final Max intMax = new Max();
        intMax.setMode(NumericAggregateFunction.NumberType.INT);
        intMax.init();

        // When 1
        intMax.aggregate(new Object[]{null});

        // Then 1
        assertNull(intMax.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirstNullInputSecond() {
        // Given
        final Max max = new Max();
        max.init();

        // When 1
        int firstValue = 1;
        max._aggregate(firstValue);

        // Then
        assertTrue(max.state()[0] instanceof Integer);
        assertEquals(firstValue, max.state()[0]);

        // When 2
        max.aggregate(new Object[]{null});

        // Then
        assertTrue(max.state()[0] instanceof Integer);
        assertEquals(firstValue, max.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirstNullInputSecond() {
        // Given
        final Max max = new Max();
        max.init();

        // When 1
        long firstValue = 1l;
        max._aggregate(firstValue);
        // Then
        assertTrue(max.state()[0] instanceof Long);
        assertEquals(firstValue, max.state()[0]);

        // When 2
        max.aggregate(new Object[]{null});
        // Then
        assertTrue(max.state()[0] instanceof Long);
        assertEquals(firstValue, max.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirstNullInputSecond() {
        // Given
        final Max max = new Max();
        max.init();

        // When 1
        double firstValue = 1.0f;
        max._aggregate(firstValue);
        // Then
        assertTrue(max.state()[0] instanceof Double);
        assertEquals(firstValue, max.state()[0]);

        // When 2
        max.aggregate(new Object[]{null});
        // Then
        assertTrue(max.state()[0] instanceof Double);
        assertEquals(firstValue, max.state()[0]);
    }

    @Test
    public void testCloneInAutoMode() {
        // Given
        final Max max = new Max();
        max.init();

        // When 1
        final Max clone = max.statelessClone();
        // Then 1
        assertNotSame(max, clone);
        assertEquals(NumericAggregateFunction.NumberType.AUTO, clone.getMode());

        // When 2
        clone._aggregate(1);
        // Then 2
        assertEquals(1, clone.state()[0]);
    }

    @Test
    public void testCloneInIntMode() {
        // Given
        final Max intMax = new Max();
        intMax.setMode(NumericAggregateFunction.NumberType.INT);
        intMax.init();

        // When 1
        final Max clone = intMax.statelessClone();
        // Then 1
        assertNotSame(intMax, clone);
        assertEquals(NumericAggregateFunction.NumberType.INT, clone.getMode());

        // When 2
        clone._aggregate(1);
        // Then 2
        assertEquals(1, clone.state()[0]);
    }

    @Test
    public void testCloneInLongMode() {
        // Given
        final Max longMax = new Max();
        longMax.setMode(NumericAggregateFunction.NumberType.LONG);
        longMax.init();

        // When 1
        final Max clone = longMax.statelessClone();
        // Then 1
        assertNotSame(longMax, clone);
        assertEquals(NumericAggregateFunction.NumberType.LONG, clone.getMode());

        // When 2
        clone._aggregate(1l);
        // Then 2
        assertEquals(1l, clone.state()[0]);
    }

    @Test
    public void testCloneInDoubleMode() {
        // Given
        final Max doubleMax = new Max();
        doubleMax.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleMax.init();

        // When 1
        final Max clone = doubleMax.statelessClone();
        // Then 2
        assertNotSame(doubleMax, clone);
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, clone.getMode());

        // When 2
        clone._aggregate(1d);
        // Then 2
        assertEquals(1d, clone.state()[0]);
    }

    @Test
    public void testCloneAfterExecute() {
        // Given
        final Max max = new Max();
        max.setMode(NumericAggregateFunction.NumberType.INT);
        max.init();
        final Integer initialState = (Integer) max.state()[0];
        max._aggregate(1);

        // When
        final Max clone = max.statelessClone();

        // Then
        assertEquals(initialState, clone.state()[0]);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Max aggregator = new Max();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.Max\",%n" +
                "  \"mode\" : \"AUTO\"%n" +
                "}"), json);

        // When 2
        final Max deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), Max.class);

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Max getInstance() {
        return new Max();
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return Max.class;
    }
}