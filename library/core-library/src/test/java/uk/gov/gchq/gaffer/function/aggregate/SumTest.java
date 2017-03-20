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

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SumTest extends BinaryOperatorTest {
    private Number state;

    @Before
    public void before() {
        state = null;
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Sum sum = new Sum();

        // When 1
        state = sum.apply(1, state);

        // Then 1
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 2
        state = sum.apply(3, state);

        // Then 2
        assertTrue(state instanceof Integer);
        assertEquals(4, state);

        // When 3
        state = sum.apply(2, state);

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(6, state);
    }

    @Test
    public void testAggregateInIntModeMixedInput() {
        // Given
        final Sum sum = new Sum();

        // When 1
        state = sum.apply(1, state);

        // Then 1
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 2
        try {
            state = sum.apply(2.7d, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 3
        try {
            state = sum.apply(1l, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(1, state);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Sum sum = new Sum();

        // When 1
        state = sum.apply(2l, state);

        // Then 1
        assertTrue(state instanceof Long);
        assertEquals(2l, state);

        // When 2
        state = sum.apply(1l, state);

        // Then 2
        assertTrue(state instanceof Long);
        assertEquals(3l, state);

        // When 3
        state = sum.apply(3l, state);

        // Then 3
        assertTrue(state instanceof Long);
        assertEquals(6l, state);
    }

    @Test
    public void testAggregateInLongModeMixedInput() {
        // Given
        final Sum sum = new Sum();
        state = 0l;

        // When 1
        try {
            state = sum.apply(1, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertEquals(0l, state);

        // When 2
        state = sum.apply(3l, state);

        // Then 2
        assertTrue(state instanceof Long);
        assertEquals(3l, state);

        // When 3
        try {
            state = sum.apply(2.5d, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Long);
        assertEquals(3l, state);
    }

    @Test
    public void testAggregateInDoubleMode() {
        // Given
        final Sum sum = new Sum();

        // When 1
        state = sum.apply(1.1d, state);

        // Then 1
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);

        // When 2
        state = sum.apply(2.1d, state);

        // Then 2
        assertTrue(state instanceof Double);
        assertEquals(3.2d, state);

        // When 3
        state = sum.apply(1.5d, state);

        // Then 3
        assertTrue(state instanceof Double);
        assertEquals(4.7d, state);
    }

    @Test
    public void testAggregateInDoubleModeMixedInput() {
        // Given
        final Sum sum = new Sum();
        state = 0d;

        // When 1
        try {
            state = sum.apply(1, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertEquals(0d, state);

        // When 2
        try {
            state = sum.apply(3l, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(0d, state);

        // When 3
        state = sum.apply(2.1d, state);

        // Then 3
        assertTrue(state instanceof Double);
        assertEquals(2.1d, state);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirst() {
        // Given
        final Sum sum = new Sum();
        state = 0;

        // When 1
        state = sum.apply(1, state);

        // Then 1
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 2
        try {
            state = sum.apply(3l, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(state instanceof Integer);
        assertEquals(1, state);

        // When 3
        try {
            state = sum.apply(2.1d, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(1, state);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirst() {
        // Given
        final Sum sum = new Sum();
        state = 0l;

        // When 1
        state = sum.apply(1l, state);

        // Then 1
        assertTrue(state instanceof Long);
        assertEquals(1l, state);

        // When 2
        try {
            state = sum.apply(3, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(state instanceof Long);
        assertEquals(1l, state);

        // When 3
        try {
            state = sum.apply(2.1d, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Long);
        assertEquals(1l, state);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirst() {
        // Given
        final Sum sum = new Sum();
        state = 0d;

        // When 1
        state = sum.apply(1.1d, state);

        // Then 1
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);

        // When 2
        try {
            state = sum.apply(2, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);

        // When 3
        try {
            state = sum.apply(1l, state);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);
    }

    @Test
    public void testAggregateInAutoModeNullFirst() {
        // Given
        final Sum sum = new Sum();


        // When 1
        state = sum.apply(null, state);
        // Then 1
        assertEquals(null, state);
    }

    @Test
    public void testAggregateInIntModeNullFirst() {
        // Given
        final Sum sum = new Sum();


        // When 1
        state = sum.apply(null, state);

        // Then 1
        assertNull(state);
    }

    @Test
    public void testAggregateInLongModeNullFirst() {
        // Given
        final Sum sum = new Sum();


        // When 1
        state = sum.apply(null, state);

        // Then 1
        assertNull(state);
    }

    @Test
    public void testAggregateInDoubleModeNullFirst() {
        // Given
        final Sum sum = new Sum();


        // When 1
        state = sum.apply(null, state);

        // Then 1
        assertNull(state);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirstNullInputSecond() {
        // Given
        final Sum sum = new Sum();


        // When 1
        int firstValue = 1;
        state = sum.apply(firstValue, state);

        // Then
        assertTrue(state instanceof Integer);
        assertEquals(firstValue, state);

        // When 2
        state = sum.apply(null, state);
        // Then
        assertTrue(state instanceof Integer);
        assertEquals(firstValue, state);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirstNullInputSecond() {
        // Given
        final Sum sum = new Sum();


        // When 1
        long firstValue = 1l;
        state = sum.apply(firstValue, state);

        // Then
        assertTrue(state instanceof Long);
        assertEquals(firstValue, state);

        // When 2
        state = sum.apply(null, state);
        // Then
        assertTrue(state instanceof Long);
        assertEquals(firstValue, state);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirstNullInputSecond() {
        // Given
        final Sum sum = new Sum();


        // When 1
        double firstValue = 1.0f;
        state = sum.apply(firstValue, state);

        // Then
        assertTrue(state instanceof Double);
        assertEquals(firstValue, state);

        // When 2
        state = sum.apply(null, state);

        // Then
        assertTrue(state instanceof Double);
        assertEquals(firstValue, state);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Sum aggregator = new Sum();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.Sum\"%n" +
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
    protected Class<Sum> getFunctionClass() {
        return Sum.class;
    }
}
