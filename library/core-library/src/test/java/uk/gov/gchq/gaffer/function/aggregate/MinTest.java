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

public class MinTest extends AggregateFunctionTest {
    @Test
    public void testInitialiseInAutoMode() {
        final Min min = new Min();

        assertEquals(NumericAggregateFunction.NumberType.AUTO, min.getMode());

        assertNull(min.state()[0]);
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Min intMin = new Min();
        intMin.setMode(NumericAggregateFunction.NumberType.INT);

        intMin.init();

        // When 1
        intMin._aggregate(1);

        // Then 1
        assertTrue(intMin.state()[0] instanceof Integer);
        assertEquals(1, intMin.state()[0]);

        // When 2
        intMin._aggregate(2);

        // Then 2
        assertTrue(intMin.state()[0] instanceof Integer);
        assertEquals(1, intMin.state()[0]);

        // When 3
        intMin._aggregate(3);

        // Then 3
        assertTrue(intMin.state()[0] instanceof Integer);
        assertEquals(1, intMin.state()[0]);
    }

    @Test
    public void testAggregateInIntModeMixedInput() {
        // Given
        final Min intMin = new Min();
        intMin.setMode(NumericAggregateFunction.NumberType.INT);

        intMin.init();

        // When 1
        intMin._aggregate(1);

        // Then 1
        assertTrue(intMin.state()[0] instanceof Integer);
        assertEquals(1, intMin.state()[0]);

        // When 2
        try {
            intMin._aggregate(2.7d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(intMin.state()[0] instanceof Integer);
        assertEquals(1, intMin.state()[0]);

        // When 3
        try {
            intMin._aggregate(5l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(intMin.state()[0] instanceof Integer);
        assertEquals(1, intMin.state()[0]);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Min longMin = new Min();
        longMin.setMode(NumericAggregateFunction.NumberType.LONG);

        longMin.init();

        // When 1
        longMin._aggregate(1l);

        // Then 1
        assertTrue(longMin.state()[0] instanceof Long);
        assertEquals(1l, longMin.state()[0]);

        // When 2
        longMin._aggregate(2l);

        // Then 2
        assertTrue(longMin.state()[0] instanceof Long);
        assertEquals(1l, longMin.state()[0]);

        // When 3
        longMin._aggregate(3l);

        // Then 3
        assertTrue(longMin.state()[0] instanceof Long);
        assertEquals(1l, longMin.state()[0]);
    }

    @Test
    public void testAggregateInLongModeMixedInput() {
        // Given
        final Min longMin = new Min();
        longMin.setMode(NumericAggregateFunction.NumberType.LONG);

        longMin.init();

        // When 1
        try {
            longMin._aggregate(3);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(longMin.state()[0]);

        // When 2
        longMin._aggregate(2l);

        // Then 2
        assertTrue(longMin.state()[0] instanceof Long);
        assertEquals(2l, longMin.state()[0]);

        // When 3
        try {
            longMin._aggregate(1.5d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(longMin.state()[0] instanceof Long);
        assertEquals(2l, longMin.state()[0]);
    }

    @Test
    public void testAggregateInDoubleMode() {
        // Given
        final Min doubleMin = new Min();
        doubleMin.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleMin.init();

        // When 1
        doubleMin._aggregate(2.1d);

        // Then 1
        assertTrue(doubleMin.state()[0] instanceof Double);
        assertEquals(2.1d, doubleMin.state()[0]);

        // When 2
        doubleMin._aggregate(1.1d);

        // Then 2
        assertTrue(doubleMin.state()[0] instanceof Double);
        assertEquals(1.1d, doubleMin.state()[0]);

        // When 3
        doubleMin._aggregate(3.1d);

        // Then 3
        assertTrue(doubleMin.state()[0] instanceof Double);
        assertEquals(1.1d, doubleMin.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeMixedInput() {
        // Given
        final Min doubleMin = new Min();
        doubleMin.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleMin.init();

        // When 1
        try {
            doubleMin._aggregate(5);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(doubleMin.state()[0]);

        // When 2
        try {
            doubleMin._aggregate(2l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertNull(doubleMin.state()[0]);

        // When 3
        doubleMin._aggregate(2.1d);

        // Then 3
        assertTrue(doubleMin.state()[0] instanceof Double);
        assertEquals(2.1d, doubleMin.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirst() {
        // Given
        final Min min = new Min();

        min.init();

        // When 1
        min._aggregate(2);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.INT, min.getMode());
        assertTrue(min.state()[0] instanceof Integer);
        assertEquals(2, min.state()[0]);

        // When 2
        try {
            min._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.INT, min.getMode());
        assertTrue(min.state()[0] instanceof Integer);
        assertEquals(2, min.state()[0]);

        // When 3
        try {
            min._aggregate(1.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.INT, min.getMode());
        assertTrue(min.state()[0] instanceof Integer);
        assertEquals(2, min.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirst() {
        // Given
        final Min min = new Min();

        min.init();

        // When 1
        min._aggregate(2l);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.LONG, min.getMode());
        assertTrue(min.state()[0] instanceof Long);
        assertEquals(2l, min.state()[0]);

        // When 2
        try {
            min._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.LONG, min.getMode());
        assertTrue(min.state()[0] instanceof Long);
        assertEquals(2l, min.state()[0]);

        // When 3
        try {
            min._aggregate(3.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.LONG, min.getMode());
        assertTrue(min.state()[0] instanceof Long);
        assertEquals(2l, min.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirst() {
        // Given
        final Min min = new Min();

        min.init();

        // When 1
        min._aggregate(2.1d);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, min.getMode());
        assertTrue(min.state()[0] instanceof Double);
        assertEquals(2.1d, min.state()[0]);

        // When 2
        try {
            min._aggregate(3);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, min.getMode());
        assertTrue(min.state()[0] instanceof Double);
        assertEquals(2.1d, min.state()[0]);

        // When 3
        try {
            min._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, min.getMode());
        assertTrue(min.state()[0] instanceof Double);
        assertEquals(2.1d, min.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeNullFirst() {
        // Given
        final Min min = new Min();
        min.init();

        // When 1
        min.aggregate(new Object[]{null});
        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.AUTO, min.getMode());
        assertEquals(null, min.state()[0]);
    }

    @Test
    public void testAggregateInIntModeNullFirst() {
        // Given
        final Min intMin = new Min();
        intMin.setMode(NumericAggregateFunction.NumberType.INT);
        intMin.init();

        // When 1
        intMin.aggregate(new Object[]{null});

        // Then 1
        assertNull(intMin.state()[0]);
    }

    @Test
    public void testAggregateInLongModeNullFirst() {
        // Given
        final Min longMin = new Min();
        longMin.setMode(NumericAggregateFunction.NumberType.LONG);
        longMin.init();

        // When 1
        longMin.aggregate(new Object[]{null});

        // Then 1
        assertNull(longMin.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeNullFirst() {
        // Given
        final Min doubleMin = new Min();
        doubleMin.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleMin.init();

        // When 1
        doubleMin.aggregate(new Object[]{null});

        // Then 1
        assertNull(doubleMin.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirstNullInputSecond() {
        // Given
        final Min min = new Min();
        min.init();

        // When 1
        int firstValue = 1;
        min._aggregate(firstValue);
        // Then
        assertTrue(min.state()[0] instanceof Integer);
        assertEquals(firstValue, min.state()[0]);

        // When 2
        min.aggregate(new Object[]{null});
        // Then
        assertTrue(min.state()[0] instanceof Integer);
        assertEquals(firstValue, min.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirstNullInputSecond() {
        // Given
        final Min min = new Min();
        min.init();

        // When 1
        long firstValue = 1l;
        min._aggregate(firstValue);
        // Then
        assertTrue(min.state()[0] instanceof Long);
        assertEquals(firstValue, min.state()[0]);

        // When 2
        min.aggregate(new Object[]{null});
        // Then
        assertTrue(min.state()[0] instanceof Long);
        assertEquals(firstValue, min.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirstNullInputSecond() {
        // Given
        final Min min = new Min();
        min.init();

        // When 1
        double firstValue = 1.0f;
        min._aggregate(firstValue);
        // Then
        assertTrue(min.state()[0] instanceof Double);
        assertEquals(firstValue, min.state()[0]);

        // When 2
        min.aggregate(new Object[]{null});
        // Then
        assertTrue(min.state()[0] instanceof Double);
        assertEquals(firstValue, min.state()[0]);
    }

    @Test
    public void testCloneInAutoMode() {
        // Given
        final Min min = new Min();
        min.init();

        // When 1
        final Min clone = min.statelessClone();
        // Then 1
        assertNotSame(min, clone);
        assertEquals(NumericAggregateFunction.NumberType.AUTO, clone.getMode());

        // When 2
        clone._aggregate(1);
        // Then 2
        assertEquals(1, clone.state()[0]);
    }

    @Test
    public void testCloneInIntMode() {
        // Given
        final Min intMin = new Min();
        intMin.setMode(NumericAggregateFunction.NumberType.INT);
        intMin.init();

        // When 1
        final Min clone = intMin.statelessClone();
        // Then 1
        assertNotSame(intMin, clone);
        assertEquals(NumericAggregateFunction.NumberType.INT, clone.getMode());

        // When 2
        clone._aggregate(1);
        // Then 2
        assertEquals(1, clone.state()[0]);
    }

    @Test
    public void testCloneInLongMode() {
        // Given
        final Min longMin = new Min();
        longMin.setMode(NumericAggregateFunction.NumberType.LONG);
        longMin.init();

        // When 1
        final Min clone = longMin.statelessClone();
        // Then 1
        assertNotSame(longMin, clone);
        assertEquals(NumericAggregateFunction.NumberType.LONG, clone.getMode());

        // When 2
        clone._aggregate(1l);
        // Then 2
        assertEquals(1l, clone.state()[0]);
    }

    @Test
    public void testCloneInDoubleMode() {
        // Given
        final Min doubleMin = new Min();
        doubleMin.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleMin.init();

        // When 1
        final Min clone = doubleMin.statelessClone();
        // Then 1
        assertNotSame(doubleMin, clone);
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, clone.getMode());

        // When 2
        clone._aggregate(1d);
        // Then 2
        assertEquals(1d, clone.state()[0]);
    }

    @Test
    public void testCloneAfterExecute() {
        // Given
        final Min min = new Min();
        min.setMode(NumericAggregateFunction.NumberType.INT);
        min.init();
        Integer initialState = (Integer) min.state()[0];
        min._aggregate(1);

        // When
        final Min clone = min.statelessClone();
        // Then
        assertEquals(initialState, clone.state()[0]);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Min aggregator = new Min();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.Min\",%n" +
                "  \"mode\" : \"AUTO\"%n" +
                "}"), json);

        // When 2
        final Min deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), Min.class);

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Min getInstance() {
        return new Min();
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return Min.class;
    }
}
