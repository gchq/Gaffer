/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.function.ExampleTuple2BinaryOperator;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;

import java.util.List;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ElementAggregatorTest {

    @Test
    public void shouldAggregateElementUsingMockBinaryOperator() {
        // Given
        final String reference = "reference1";
        final Integer valueResult = 3;

        final BinaryOperator<Integer> function = mock(BinaryOperator.class);
        given(function.apply(1, 2)).willReturn(valueResult);

        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(reference)
                .execute(function)
                .build();

        final Edge edge1 = new Edge.Builder()
                .property(reference, 1)
                .build();

        final Edge edge2 = new Edge.Builder()
                .property(reference, 2)
                .build();

        // When
        final Element result = aggregator.apply(edge1, edge2);

        // Then
        assertEquals(valueResult, result.getProperty(reference));
    }

    @Test
    public void shouldAggregateElementUsingLambdaBinaryOperator() {
        // Given
        final String reference = "reference1";

        final BinaryOperator<String> function = (a, b) -> a + "," + b;
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(reference)
                .execute(function)
                .build();

        final Edge edge1 = new Edge.Builder()
                .property(reference, "value1")
                .build();

        final Edge edge2 = new Edge.Builder()
                .property(reference, "value2")
                .build();

        // When
        final Element result = aggregator.apply(edge1, edge2);

        // Then
        assertEquals("value1,value2", result.getProperty(reference));
    }

    @Test
    public void shouldAggregateElementUsingKorypheBinaryOperator() {
        // Given
        final String reference = "reference1";

        final BinaryOperator<Integer> function = new KorypheBinaryOperator<Integer>() {
            @Override
            public Integer _apply(final Integer a, final Integer b) {
                return a + b;
            }
        };

        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(reference)
                .execute(function)
                .build();

        final Edge edge1 = new Edge.Builder()
                .property(reference, 1)
                .build();

        final Edge edge2 = new Edge.Builder()
                .property(reference, 2)
                .build();

        // When
        final Element result = aggregator.apply(edge1, edge2);

        // Then
        assertEquals(3, result.getProperty(reference));
    }

    @Test
    public void shouldAggregateProperties() {
        // Given
        final String reference = "reference1";
        final Integer value1 = 1;
        final Integer value2 = 2;
        final Integer valueResult = 3;

        final BinaryOperator<Integer> function = mock(BinaryOperator.class);
        given(function.apply(value1, value2)).willReturn(valueResult);

        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(reference)
                .execute(function)
                .build();

        final Properties properties1 = new Properties(reference, value1);
        final Properties properties2 = new Properties(reference, value2);

        // When
        final Properties result = aggregator.apply(properties1, properties2);

        // Then
        assertEquals(valueResult, result.get(reference));
    }

    @Test
    public void shouldAggregatePropertiesWithMultipleOfFunctions() {
        // Given
        final BinaryOperator<Integer> max = Math::max;
        final BinaryOperator<Integer> min = Math::min;

        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select("max")
                .execute(max)
                .select("min")
                .execute(min)
                .build();

        final Properties properties1 = new Properties();
        properties1.put("max", 10);
        properties1.put("min", 10);

        final Properties properties2 = new Properties();
        properties2.put("max", 100);
        properties2.put("min", 100);

        final Properties properties3 = new Properties();
        properties3.put("max", 1000);
        properties3.put("min", 1000);

        // When
        Properties state = aggregator.apply(properties1, properties2);
        state = aggregator.apply(state, properties3);

        // Then
        assertEquals(1000, state.get("max"));
        assertEquals(10, state.get("min"));
    }

    @Test
    public void shouldAggregatePropertiesWithMultipleSelection() {
        // Given
        final BinaryOperator<Tuple3<Integer, Integer, Integer>> maxMinRange =
                (t1, t2) -> new Tuple3<>(
                        Math.max(t1.get0(), t2.get0()),
                        Math.min(t1.get1(), t2.get1()),
                        Math.max(t1.get0(), t2.get0()) - Math.min(t1.get1(), t2.get1())
                );

        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select("max", "min", "range")
                .execute(maxMinRange)
                .build();

        final Properties properties1 = new Properties();
        properties1.put("max", 10);
        properties1.put("min", 10);

        final Properties properties2 = new Properties();
        properties2.put("max", 100);
        properties2.put("min", 100);

        final Properties properties3 = new Properties();
        properties3.put("max", 1000);
        properties3.put("min", 1000);

        // When
        Properties state = aggregator.apply(properties1, properties2);
        state = aggregator.apply(state, properties3);

        // Then
        assertEquals(1000, state.get("max"));
        assertEquals(10, state.get("min"));
        assertEquals(1000 - 10, state.get("range"));
    }

    @Test
    public void shouldAggregateWithNoPropertiesOrFunctions() {
        // Given
        final ElementAggregator aggregator = new ElementAggregator();
        final Edge edge1 = new Edge.Builder().group("group").build();
        final Edge edge2 = new Edge.Builder().group("group").build();

        // When - aggregate and set state
        final Element result = aggregator.apply(edge1, edge2);

        // Then
        assertEquals(edge2, result);
        assertTrue(result.getProperties().isEmpty());
    }

    @Test
    public void shouldBuildAggregator() {
        // Given
        final String property1 = "property 1";
        final String property2a = "property 2a";
        final String property2b = "property 2b";
        final String property3 = "property 3";

        final BinaryOperator func1 = mock(BinaryOperator.class);
        final BinaryOperator func2 = mock(BinaryOperator.class);
        final BinaryOperator func3 = mock(BinaryOperator.class);

        // When - check you can build the selection/function in any order,
        // although normally it will be done - select then execute.
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(property1)
                .execute(func1)
                .select(property2a, property2b)
                .execute(func2)
                .select(property3)
                .execute(func3)
                .build();

        // Then
        int i = 0;
        TupleAdaptedBinaryOperator<String, ?> adaptedFunction = aggregator.getComponents().get(i++);
        assertEquals(1, adaptedFunction.getSelection().length);
        assertEquals(property1, adaptedFunction.getSelection()[0]);
        assertSame(func1, adaptedFunction.getBinaryOperator());

        adaptedFunction = aggregator.getComponents().get(i++);
        assertEquals(2, adaptedFunction.getSelection().length);
        assertEquals(property2a, adaptedFunction.getSelection()[0]);
        assertEquals(property2b, adaptedFunction.getSelection()[1]);
        assertSame(func2, adaptedFunction.getBinaryOperator());

        adaptedFunction = aggregator.getComponents().get(i++);
        assertSame(func3, adaptedFunction.getBinaryOperator());
        assertEquals(1, adaptedFunction.getSelection().length);
        assertEquals(property3, adaptedFunction.getSelection()[0]);

        assertEquals(i, aggregator.getComponents().size());
    }

    @Test
    public void shouldAggregateWithTuple2BinaryOperator() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final BinaryOperator func1 = new ExampleTuple2BinaryOperator();
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(property1, property2)
                .execute(func1)
                .build();

        final Properties props1 = new Properties();
        props1.put(property1, 1);
        props1.put(property2, "value1");

        final Properties props2 = new Properties();
        props2.put(property1, 10);
        props2.put(property2, "value10");

        final Properties props3 = new Properties();
        props3.put(property1, 5);
        props3.put(property2, "value5");

        // When
        Properties state = props1;
        state = aggregator.apply(state, props2);
        state = aggregator.apply(state, props3);

        // Then
        assertEquals(props2, state);
    }

    @Test
    public void shouldReturnUnmodifiableComponentsWhenLocked() {
        // Given
        final ElementAggregator aggregator = new ElementAggregator();

        // When
        aggregator.lock();
        final List<TupleAdaptedBinaryOperator<String, ?>> components = aggregator.getComponents();

        // Then
        try {
            components.add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldReturnModifiableComponentsWhenNotLocked() {
        // Given
        final ElementAggregator aggregator = new ElementAggregator();

        // When
        final List<TupleAdaptedBinaryOperator<String, ?>> components = aggregator.getComponents();

        // Then - no exceptions
        components.add(null);
    }
}
