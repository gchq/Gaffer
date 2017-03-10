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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProductTest extends BinaryOperatorTest {
    private Number state;

    @Before
    public void before() {
        state = null;
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.INT);

        // When 1
        state = product.apply(state, 2);

        // Then 1
        assertTrue(state instanceof Integer);
        assertEquals(2, state);

        // When 2
        state = product.apply(state, 3);

        // Then 2
        assertTrue(state instanceof Integer);
        assertEquals(6, state);

        // When 3
        state = product.apply(state, 8);

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(48, state);
    }

    @Test
    public void testAggregateInIntModeMixedInput() {
        // Given
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.INT);


        // When 1
        state = product.apply(state, 2);

        // Then 1
        assertTrue(state instanceof Integer);
        assertEquals(2, state);

        // When 2
        try {
            state = product.apply(state, 2.7d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(state instanceof Integer);
        assertEquals(2, state);

        // When 3
        try {
            state = product.apply(state, 1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(state instanceof Integer);
        assertEquals(2, state);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.LONG);

        // When 1
        state = product.apply(state, 2l);

        // Then 1
        assertTrue(state instanceof Long);
        assertEquals(2l, state);

        // When 2
        state = product.apply(state, 1l);

        // Then 2
        assertTrue(state instanceof Long);
        assertEquals(2l, state);

        // When 3
        state = product.apply(state, 3l);

        // Then 3
        assertTrue(state instanceof Long);
        assertEquals(6l, state);
    }

    @Test
    public void testAggregateInLongModeMixedInput() {
        // Given
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.LONG);
        state = 1l;

        // When 1
        try {
            state = product.apply(state, 1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertEquals(1l, state);

        // When 2
        state = product.apply(state, 3l);

        // Then 2
        assertTrue(state instanceof Long);
        assertEquals(3l, state);

        // When 3
        try {
            state = product.apply(state, 2.5d);
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
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        // When 1
        state = product.apply(state, 1.2d);

        // Then 1
        assertTrue(state instanceof Double);
        assertEquals(1.2d, state);

        // When 2
        state = product.apply(state, 2.5d);

        // Then 2
        assertTrue(state instanceof Double);
        assertEquals(3.0d, state);

        // When 3
        state = product.apply(state, 1.5d);

        // Then 3
        assertTrue(state instanceof Double);
        assertEquals(4.5d, state);
    }

    @Test
    public void testAggregateInDoubleModeMixedInput() {
        // Given
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        state = 1d;

        // When 1
        try {
            state = product.apply(state, 1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertEquals(1d, state);

        // When 2
        try {
            state = product.apply(state, 3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(1d, state);

        // When 3
        state = product.apply(state, 2.1d);

        // Then 3
        assertTrue(state instanceof Double);
        assertEquals(2.1d, state);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirst() {
        // Given
        final Product product = new Product();
        state = 1;

        // When 1
        state = product.apply(state, 2);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.INT, product.getMode());
        assertTrue(state instanceof Integer);
        assertEquals(2, state);

        // When 2
        try {
            state = product.apply(state, 3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.INT, product.getMode());
        assertTrue(state instanceof Integer);
        assertEquals(2, state);

        // When 3
        try {
            state = product.apply(state, 2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.INT, product.getMode());
        assertTrue(state instanceof Integer);
        assertEquals(2, state);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirst() {
        // Given
        final Product product = new Product();
        state = 1l;

        // When 1
        state = product.apply(state, 2l);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.LONG, product.getMode());
        assertTrue(state instanceof Long);
        assertEquals(2l, state);

        // When 2
        try {
            state = product.apply(state, 3);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.LONG, product.getMode());
        assertTrue(state instanceof Long);
        assertEquals(2l, state);

        // When 3
        try {
            state = product.apply(state, 2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.LONG, product.getMode());
        assertTrue(state instanceof Long);
        assertEquals(2l, state);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirst() {
        // Given
        final Product product = new Product();
        state = 1d;

        // When 1
        state = product.apply(state, 1.1d);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, product.getMode());
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);

        // When 2
        try {
            state = product.apply(state, 2);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, product.getMode());
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);

        // When 3
        try {
            state = product.apply(state, 1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, product.getMode());
        assertTrue(state instanceof Double);
        assertEquals(1.1d, state);
    }


    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Product aggregator = new Product();

        // When 1
        final String json = new String(new JSONSerialiser().serialise(aggregator, true));

        // Then 1
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.aggregate.Product\",%n" +
                "  \"mode\" : \"AUTO\"%n" +
                "}"), json);

        // When 2
        final Product deserialisedAggregator = new JSONSerialiser().deserialise(json.getBytes(), Product.class);

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected Product getInstance() {
        return new Product();
    }

    @Override
    protected Class<Product> getFunctionClass() {
        return Product.class;
    }
}
