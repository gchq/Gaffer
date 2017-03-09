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

public class SumTest extends AggregateFunctionTest {

    @Test
    public void testInitialiseInAutoMode() {
        final Sum sum = new Sum();

        assertEquals(NumericAggregateFunction.NumberType.AUTO, sum.getMode());

        assertNull(sum.state()[0]);
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Sum intSum = new Sum();
        intSum.setMode(NumericAggregateFunction.NumberType.INT);

        intSum.init();

        // When 1
        intSum._aggregate(1);

        // Then 1
        assertTrue(intSum.state()[0] instanceof Integer);
        assertEquals(1, intSum.state()[0]);

        // When 2
        intSum._aggregate(3);

        // Then 2
        assertTrue(intSum.state()[0] instanceof Integer);
        assertEquals(4, intSum.state()[0]);

        // When 3
        intSum._aggregate(2);

        // Then 3
        assertTrue(intSum.state()[0] instanceof Integer);
        assertEquals(6, intSum.state()[0]);
    }

    @Test
    public void testAggregateInIntModeMixedInput() {
        // Given
        final Sum intSum = new Sum();
        intSum.setMode(NumericAggregateFunction.NumberType.INT);

        intSum.init();

        // When 1
        intSum._aggregate(1);

        // Then 1
        assertTrue(intSum.state()[0] instanceof Integer);
        assertEquals(1, intSum.state()[0]);

        // When 2
        try {
            intSum._aggregate(2.7d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(intSum.state()[0] instanceof Integer);
        assertEquals(1, intSum.state()[0]);

        // When 3
        try {
            intSum._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(intSum.state()[0] instanceof Integer);
        assertEquals(1, intSum.state()[0]);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Sum longSum = new Sum();
        longSum.setMode(NumericAggregateFunction.NumberType.LONG);

        longSum.init();

        // When 1
        longSum._aggregate(2l);

        // Then 1
        assertTrue(longSum.state()[0] instanceof Long);
        assertEquals(2l, longSum.state()[0]);

        // When 2
        longSum._aggregate(1l);

        // Then 2
        assertTrue(longSum.state()[0] instanceof Long);
        assertEquals(3l, longSum.state()[0]);

        // When 3
        longSum._aggregate(3l);

        // Then 3
        assertTrue(longSum.state()[0] instanceof Long);
        assertEquals(6l, longSum.state()[0]);
    }

    @Test
    public void testAggregateInLongModeMixedInput() {
        // Given
        final Sum longSum = new Sum();
        longSum.setMode(NumericAggregateFunction.NumberType.LONG);

        longSum.init();

        // When 1
        try {
            longSum._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(longSum.state()[0]);

        // When 2
        longSum._aggregate(3l);

        // Then 2
        assertTrue(longSum.state()[0] instanceof Long);
        assertEquals(3l, longSum.state()[0]);

        // When 3
        try {
            longSum._aggregate(2.5d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(longSum.state()[0] instanceof Long);
        assertEquals(3l, longSum.state()[0]);
    }

    @Test
    public void testAggregateInDoubleMode() {
        // Given
        final Sum doubleSum = new Sum();
        doubleSum.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleSum.init();

        // When 1
        doubleSum._aggregate(1.1d);

        // Then 1
        assertTrue(doubleSum.state()[0] instanceof Double);
        assertEquals(1.1d, doubleSum.state()[0]);

        // When 2
        doubleSum._aggregate(2.1d);

        // Then 2
        assertTrue(doubleSum.state()[0] instanceof Double);
        assertEquals(3.2d, doubleSum.state()[0]);

        // When 3
        doubleSum._aggregate(1.5d);

        // Then 3
        assertTrue(doubleSum.state()[0] instanceof Double);
        assertEquals(4.7d, doubleSum.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeMixedInput() {
        // Given
        final Sum doubleSum = new Sum();
        doubleSum.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleSum.init();

        // When 1
        try {
            doubleSum._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(doubleSum.state()[0]);

        // When 2
        try {
            doubleSum._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertNull(doubleSum.state()[0]);

        // When 3
        doubleSum._aggregate(2.1d);

        // Then 3
        assertTrue(doubleSum.state()[0] instanceof Double);
        assertEquals(2.1d, doubleSum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirst() {
        // Given
        final Sum sum = new Sum();

        sum.init();

        // When 1
        sum._aggregate(1);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.INT, sum.getMode());
        assertTrue(sum.state()[0] instanceof Integer);
        assertEquals(1, sum.state()[0]);

        // When 2
        try {
            sum._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.INT, sum.getMode());
        assertTrue(sum.state()[0] instanceof Integer);
        assertEquals(1, sum.state()[0]);

        // When 3
        try {
            sum._aggregate(2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.INT, sum.getMode());
        assertTrue(sum.state()[0] instanceof Integer);
        assertEquals(1, sum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirst() {
        // Given
        final Sum sum = new Sum();

        sum.init();

        // When 1
        sum._aggregate(1l);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.LONG, sum.getMode());
        assertTrue(sum.state()[0] instanceof Long);
        assertEquals(1l, sum.state()[0]);

        // When 2
        try {
            sum._aggregate(3);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.LONG, sum.getMode());
        assertTrue(sum.state()[0] instanceof Long);
        assertEquals(1l, sum.state()[0]);

        // When 3
        try {
            sum._aggregate(2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.LONG, sum.getMode());
        assertTrue(sum.state()[0] instanceof Long);
        assertEquals(1l, sum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirst() {
        // Given
        final Sum sum = new Sum();

        sum.init();

        // When 1
        sum._aggregate(1.1d);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, sum.getMode());
        assertTrue(sum.state()[0] instanceof Double);
        assertEquals(1.1d, sum.state()[0]);

        // When 2
        try {
            sum._aggregate(2);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, sum.getMode());
        assertTrue(sum.state()[0] instanceof Double);
        assertEquals(1.1d, sum.state()[0]);

        // When 3
        try {
            sum._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, sum.getMode());
        assertTrue(sum.state()[0] instanceof Double);
        assertEquals(1.1d, sum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeNullFirst() {
        // Given
        final Sum sum = new Sum();
        sum.init();

        // When 1
        sum._aggregate(null);
        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.AUTO, sum.getMode());
        assertEquals(null, sum.state()[0]);
    }

    @Test
    public void testAggregateInIntModeNullFirst() {
        // Given
        final Sum intSum = new Sum();
        intSum.setMode(NumericAggregateFunction.NumberType.INT);
        intSum.init();

        // When 1
        intSum._aggregate(null);

        // Then 1
        assertNull(intSum.state()[0]);
    }

    @Test
    public void testAggregateInLongModeNullFirst() {
        // Given
        final Sum longSum = new Sum();
        longSum.setMode(NumericAggregateFunction.NumberType.LONG);
        longSum.init();

        // When 1
        longSum._aggregate(null);

        // Then 1
        assertNull(longSum.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeNullFirst() {
        // Given
        final Sum doubleSum = new Sum();
        doubleSum.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleSum.init();

        // When 1
        doubleSum._aggregate(null);

        // Then 1
        assertNull(doubleSum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirstNullInputSecond() {
        // Given
        final Sum sum = new Sum();
        sum.init();

        // When 1
        int firstValue = 1;
        sum._aggregate(firstValue);

        // Then
        assertTrue(sum.state()[0] instanceof Integer);
        assertEquals(firstValue, sum.state()[0]);

        // When 2
        sum._aggregate(null);
        // Then
        assertTrue(sum.state()[0] instanceof Integer);
        assertEquals(firstValue, sum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirstNullInputSecond() {
        // Given
        final Sum sum = new Sum();
        sum.init();

        // When 1
        long firstValue = 1l;
        sum._aggregate(firstValue);

        // Then
        assertTrue(sum.state()[0] instanceof Long);
        assertEquals(firstValue, sum.state()[0]);

        // When 2
        sum._aggregate(null);
        // Then
        assertTrue(sum.state()[0] instanceof Long);
        assertEquals(firstValue, sum.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirstNullInputSecond() {
        // Given
        final Sum sum = new Sum();
        sum.init();

        // When 1
        double firstValue = 1.0f;
        sum._aggregate(firstValue);

        // Then
        assertTrue(sum.state()[0] instanceof Double);
        assertEquals(firstValue, sum.state()[0]);

        // When 2
        sum._aggregate(null);

        // Then
        assertTrue(sum.state()[0] instanceof Double);
        assertEquals(firstValue, sum.state()[0]);
    }

    @Test
    public void testCloneInAutoMode() {
        // Given
        final Sum sum = new Sum();
        sum.init();

        // When 1
        final Sum clone = sum.statelessClone();
        // Then 1
        assertNotSame(sum, clone);
        assertEquals(NumericAggregateFunction.NumberType.AUTO, clone.getMode());

        // When 2
        clone._aggregate(1);
        // Then 2
        assertEquals(1, clone.state()[0]);
    }

    @Test
    public void testCloneInIntMode() {
        // Given
        final Sum intSum = new Sum();
        intSum.setMode(NumericAggregateFunction.NumberType.INT);
        intSum.init();

        // When 1
        final Sum clone = intSum.statelessClone();
        // Then 1
        assertNotSame(intSum, clone);
        assertEquals(NumericAggregateFunction.NumberType.INT, clone.getMode());

        // When 2
        clone._aggregate(1);
        // Then 2
        assertEquals(1, clone.state()[0]);
    }

    @Test
    public void testCloneInLongMode() {
        // Given
        final Sum longSum = new Sum();
        longSum.setMode(NumericAggregateFunction.NumberType.LONG);
        longSum.init();

        // When 1
        final Sum clone = longSum.statelessClone();
        // Then 1
        assertNotSame(longSum, clone);
        assertEquals(NumericAggregateFunction.NumberType.LONG, clone.getMode());

        // When 2
        clone._aggregate(1l);
        // Then 2
        assertEquals(1l, clone.state()[0]);
    }

    @Test
    public void testCloneInDoubleMode() {
        // Given
        final Sum doubleSum = new Sum();
        doubleSum.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleSum.init();

        // When 1
        final Sum clone = doubleSum.statelessClone();
        // Then 1
        assertNotSame(doubleSum, clone);
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, clone.getMode());

        // When 2
        clone._aggregate(1d);
        // Then 2
        assertEquals(1d, clone.state()[0]);
    }

    @Test
    public void testCloneAfterExecute() {
        // Given
        final Sum sum = new Sum();
        sum.setMode(NumericAggregateFunction.NumberType.INT);
        sum.init();
        Integer initialState = (Integer) sum.state()[0];
        sum._aggregate(1);

        // When
        final Sum clone = sum.statelessClone();
        // Then
        assertEquals(initialState, clone.state()[0]);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Sum aggregator = new Sum();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.Sum\",%n" +
                "  \"mode\" : \"AUTO\"%n" +
                "}"), json);

        // When 2
        final Sum deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), Sum.class);

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Sum getInstance() {
        return new Sum();
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return Sum.class;
    }
}
