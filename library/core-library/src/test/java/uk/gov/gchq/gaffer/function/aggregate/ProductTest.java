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

public class ProductTest extends AggregateFunctionTest {

    @Test
    public void testInitialiseInAutoMode() {
        final Product product = new Product();

        assertEquals(NumericAggregateFunction.NumberType.AUTO, product.getMode());

        assertNull(product.state()[0]);
    }

    @Test
    public void testAggregateInIntMode() {
        // Given
        final Product intProduct = new Product();
        intProduct.setMode(NumericAggregateFunction.NumberType.INT);

        intProduct.init();

        // When 1
        intProduct._aggregate(2);

        // Then 1
        assertTrue(intProduct.state()[0] instanceof Integer);
        assertEquals(2, intProduct.state()[0]);

        // When 2
        intProduct._aggregate(3);

        // Then 2
        assertTrue(intProduct.state()[0] instanceof Integer);
        assertEquals(6, intProduct.state()[0]);

        // When 3
        intProduct._aggregate(8);

        // Then 3
        assertTrue(intProduct.state()[0] instanceof Integer);
        assertEquals(48, intProduct.state()[0]);
    }

    @Test
    public void testAggregateInIntModeMixedInput() {
        // Given
        final Product intProduct = new Product();
        intProduct.setMode(NumericAggregateFunction.NumberType.INT);

        intProduct.init();

        // When 1
        intProduct._aggregate(2);

        // Then 1
        assertTrue(intProduct.state()[0] instanceof Integer);
        assertEquals(2, intProduct.state()[0]);

        // When 2
        try {
            intProduct._aggregate(2.7d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertTrue(intProduct.state()[0] instanceof Integer);
        assertEquals(2, intProduct.state()[0]);

        // When 3
        try {
            intProduct._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(intProduct.state()[0] instanceof Integer);
        assertEquals(2, intProduct.state()[0]);
    }

    @Test
    public void testAggregateInLongMode() {
        // Given
        final Product longProduct = new Product();
        longProduct.setMode(NumericAggregateFunction.NumberType.LONG);

        longProduct.init();

        // When 1
        longProduct._aggregate(2l);

        // Then 1
        assertTrue(longProduct.state()[0] instanceof Long);
        assertEquals(2l, longProduct.state()[0]);

        // When 2
        longProduct._aggregate(1l);

        // Then 2
        assertTrue(longProduct.state()[0] instanceof Long);
        assertEquals(2l, longProduct.state()[0]);

        // When 3
        longProduct._aggregate(3l);

        // Then 3
        assertTrue(longProduct.state()[0] instanceof Long);
        assertEquals(6l, longProduct.state()[0]);
    }

    @Test
    public void testAggregateInLongModeMixedInput() {
        // Given
        final Product longProduct = new Product();
        longProduct.setMode(NumericAggregateFunction.NumberType.LONG);

        longProduct.init();

        // When 1
        try {
            longProduct._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(longProduct.state()[0]);

        // When 2
        longProduct._aggregate(3l);

        // Then 2
        assertTrue(longProduct.state()[0] instanceof Long);
        assertEquals(3l, longProduct.state()[0]);

        // When 3
        try {
            longProduct._aggregate(2.5d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertTrue(longProduct.state()[0] instanceof Long);
        assertEquals(3l, longProduct.state()[0]);
    }

    @Test
    public void testAggregateInDoubleMode() {
        // Given
        final Product doubleProduct = new Product();
        doubleProduct.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleProduct.init();

        // When 1
        doubleProduct._aggregate(1.2d);

        // Then 1
        assertTrue(doubleProduct.state()[0] instanceof Double);
        assertEquals(1.2d, doubleProduct.state()[0]);

        // When 2
        doubleProduct._aggregate(2.5d);

        // Then 2
        assertTrue(doubleProduct.state()[0] instanceof Double);
        assertEquals(3.0d, doubleProduct.state()[0]);

        // When 3
        doubleProduct._aggregate(1.5d);

        // Then 3
        assertTrue(doubleProduct.state()[0] instanceof Double);
        assertEquals(4.5d, doubleProduct.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeMixedInput() {
        // Given
        final Product doubleProduct = new Product();
        doubleProduct.setMode(NumericAggregateFunction.NumberType.DOUBLE);

        doubleProduct.init();

        // When 1
        try {
            doubleProduct._aggregate(1);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 1
        assertNull(doubleProduct.state()[0]);

        // When 2
        try {
            doubleProduct._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertNull(doubleProduct.state()[0]);

        // When 3
        doubleProduct._aggregate(2.1d);

        // Then 3
        assertTrue(doubleProduct.state()[0] instanceof Double);
        assertEquals(2.1d, doubleProduct.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirst() {
        // Given
        final Product product = new Product();

        product.init();

        // When 1
        product._aggregate(2);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.INT, product.getMode());
        assertTrue(product.state()[0] instanceof Integer);
        assertEquals(2, product.state()[0]);

        // When 2
        try {
            product._aggregate(3l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.INT, product.getMode());
        assertTrue(product.state()[0] instanceof Integer);
        assertEquals(2, product.state()[0]);

        // When 3
        try {
            product._aggregate(2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.INT, product.getMode());
        assertTrue(product.state()[0] instanceof Integer);
        assertEquals(2, product.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirst() {
        // Given
        final Product product = new Product();

        product.init();

        // When 1
        product._aggregate(2l);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.LONG, product.getMode());
        assertTrue(product.state()[0] instanceof Long);
        assertEquals(2l, product.state()[0]);

        // When 2
        try {
            product._aggregate(3);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.LONG, product.getMode());
        assertTrue(product.state()[0] instanceof Long);
        assertEquals(2l, product.state()[0]);

        // When 3
        try {
            product._aggregate(2.1d);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.LONG, product.getMode());
        assertTrue(product.state()[0] instanceof Long);
        assertEquals(2l, product.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirst() {
        // Given
        final Product product = new Product();

        product.init();

        // When 1
        product._aggregate(1.1d);

        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, product.getMode());
        assertTrue(product.state()[0] instanceof Double);
        assertEquals(1.1d, product.state()[0]);

        // When 2
        try {
            product._aggregate(2);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 2
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, product.getMode());
        assertTrue(product.state()[0] instanceof Double);
        assertEquals(1.1d, product.state()[0]);

        // When 3
        try {
            product._aggregate(1l);
            fail();
        } catch (ClassCastException cce) {
        }

        // Then 3
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, product.getMode());
        assertTrue(product.state()[0] instanceof Double);
        assertEquals(1.1d, product.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeNullFirst() {
        // Given
        final Product product = new Product();
        product.init();

        // When 1
        product.aggregate(new Object[]{null});
        // Then 1
        assertEquals(NumericAggregateFunction.NumberType.AUTO, product.getMode());
        assertEquals(null, product.state()[0]);
    }

    @Test
    public void testAggregateInIntModeNullFirst() {
        // Given
        final Product intProduct = new Product();
        intProduct.setMode(NumericAggregateFunction.NumberType.INT);
        intProduct.init();

        // When 1
        intProduct.aggregate(new Object[]{null});

        // Then 1
        assertNull(intProduct.state()[0]);
    }

    @Test
    public void testAggregateInLongModeNullFirst() {
        // Given
        final Product longProduct = new Product();
        longProduct.setMode(NumericAggregateFunction.NumberType.LONG);
        longProduct.init();

        // When 1
        longProduct.aggregate(new Object[]{null});

        // Then 1
        assertNull(longProduct.state()[0]);
    }

    @Test
    public void testAggregateInDoubleModeNullFirst() {
        // Given
        final Product doubleProduct = new Product();
        doubleProduct.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleProduct.init();

        // When 1
        doubleProduct.aggregate(new Object[]{null});

        // Then 1
        assertNull(doubleProduct.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeIntInputFirstNullInputSecond() {
        // Given
        final Product product = new Product();
        product.init();

        // When 1
        int firstValue = 1;
        product._aggregate(firstValue);
        // Then
        assertTrue(product.state()[0] instanceof Integer);
        assertEquals(firstValue, product.state()[0]);

        // When 2
        product.aggregate(new Object[]{null});

        // Then
        assertTrue(product.state()[0] instanceof Integer);
        assertEquals(firstValue, product.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeLongInputFirstNullInputSecond() {
        // Given
        final Product product = new Product();
        product.init();

        // When 1
        long firstValue = 1l;
        product._aggregate(firstValue);
        // Then
        assertTrue(product.state()[0] instanceof Long);
        assertEquals(firstValue, product.state()[0]);

        // When 2
        product.aggregate(new Object[]{null});
        // Then
        assertTrue(product.state()[0] instanceof Long);
        assertEquals(firstValue, product.state()[0]);
    }

    @Test
    public void testAggregateInAutoModeDoubleInputFirstNullInputSecond() {
        // Given
        final Product product = new Product();
        product.init();

        // When 1
        double firstValue = 1.0f;
        product._aggregate(firstValue);
        // Then
        assertTrue(product.state()[0] instanceof Double);
        assertEquals(firstValue, product.state()[0]);

        // When 2
        product.aggregate(new Object[]{null});
        // Then
        assertTrue(product.state()[0] instanceof Double);
        assertEquals(firstValue, product.state()[0]);
    }

    @Test
    public void testCloneInAutoMode() {
        // Given
        final Product product = new Product();
        product.init();

        // When 1
        final Product clone = product.statelessClone();
        // Then 1
        assertNotSame(product, clone);
        assertEquals(NumericAggregateFunction.NumberType.AUTO, clone.getMode());

        // When 2
        // This is set to 2 because 1*1 = 1 which is also the initial value for Product.
        clone._aggregate(2);
        // Then 2
        assertEquals(2, clone.state()[0]);
    }

    @Test
    public void testCloneInIntMode() {
        // Given
        final Product intProduct = new Product();
        intProduct.setMode(NumericAggregateFunction.NumberType.INT);
        intProduct.init();

        // When 1
        final Product clone = intProduct.statelessClone();
        // Then 1
        assertNotSame(intProduct, clone);
        assertEquals(NumericAggregateFunction.NumberType.INT, clone.getMode());

        // When 2
        // This is set to 2 because 1*1 = 1 which is also the initial value for Product.
        clone._aggregate(2);
        // Then 2
        assertEquals(2, clone.state()[0]);
    }

    @Test
    public void testCloneInLongMode() {
        // Given
        final Product longProduct = new Product();
        longProduct.setMode(NumericAggregateFunction.NumberType.LONG);
        longProduct.init();

        // When 1
        final Product clone = longProduct.statelessClone();
        // Then 1
        assertNotSame(longProduct, clone);
        assertEquals(NumericAggregateFunction.NumberType.LONG, clone.getMode());

        // When 2
        // This is set to 2 because 1*1 = 1 which is also the initial value for Product.
        clone._aggregate(2l);
        // Then 2
        assertEquals(2l, clone.state()[0]);
    }

    @Test
    public void testCloneInDoubleMode() {
        // Given
        final Product doubleProduct = new Product();
        doubleProduct.setMode(NumericAggregateFunction.NumberType.DOUBLE);
        doubleProduct.init();

        // When 1
        final Product clone = doubleProduct.statelessClone();
        // Then 1
        assertNotSame(doubleProduct, clone);
        assertEquals(NumericAggregateFunction.NumberType.DOUBLE, clone.getMode());

        // When 2
        // This is set to 2 because 1*1 = 1 which is also the initial value for Product.
        clone._aggregate(2d);
        // Then 2
        assertEquals(2d, clone.state()[0]);
    }

    @Test
    public void testCloneAfterExecute() {
        // Given
        final Product product = new Product();
        product.setMode(NumericAggregateFunction.NumberType.INT);
        product.init();
        Integer initialState = (Integer) product.state()[0];
        // This is set to 2 because 1*1 = 1 which is also the initial value for Product.
        product._aggregate(2);

        // When
        final Product clone = product.statelessClone();
        // Then
        assertEquals(initialState, clone.state()[0]);
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
    protected Class<? extends Function> getFunctionClass() {
        return Product.class;
    }
}
