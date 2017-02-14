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

package uk.gov.gchq.gaffer.data.element.function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.PropertiesTuple;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.context.PassThroughFunctionContext;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ElementAggregatorTest {

    @Test
    public void shouldWrapElementInElementTupleAndCallSuper() {
        // Given
        final String reference = "reference1";
        final String value = "value";
        final ElementAggregator aggregator = new ElementAggregator();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        final List<String> references = Collections.singletonList(reference);
        given(functionContext1.getSelection()).willReturn(references);

        aggregator.addFunction(functionContext1);

        final Edge edge = new Edge("group");
        edge.putProperty(reference, value);

        final ArgumentCaptor<PropertiesTuple> propertiesTupleCaptor = ArgumentCaptor.forClass(PropertiesTuple.class);
        given(functionContext1.select(propertiesTupleCaptor.capture())).willReturn(new String[]{value});

        // When
        aggregator.aggregate(edge);

        // Then
        assertEquals(edge.getProperties(), propertiesTupleCaptor.getValue().getProperties());
        verify(functionContext1, times(2)).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function).aggregate(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);
    }

    @Test
    public void shouldWrapPropertiesInPropertiesTupleAndCallSuper() {
        // Given
        final String reference = "reference1";
        final String value = "value";
        final ElementAggregator aggregator = new ElementAggregator();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        final List<String> references = Collections.singletonList(reference);
        given(functionContext1.getSelection()).willReturn(references);

        aggregator.addFunction(functionContext1);

        final Properties properties = new Properties(reference, value);
        final ArgumentCaptor<PropertiesTuple> propertiesTupleCaptor = ArgumentCaptor.forClass(PropertiesTuple.class);
        given(functionContext1.select(propertiesTupleCaptor.capture())).willReturn(new String[]{value});

        // When
        aggregator.aggregate(properties);

        // Then
        verify(functionContext1, times(2)).getFunction();

        assertSame(properties, propertiesTupleCaptor.getValue().getProperties());
        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function).aggregate(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);
    }

    @Test
    public void shouldAggregateWithNoPropertiesOrFunctions() {
        // Given
        final ElementAggregator aggregator = new ElementAggregator();
        final Edge edge1 = new Edge("group");
        final Edge edge2 = new Edge("group");

        // When - aggregate and set state
        aggregator.aggregate(edge1);
        aggregator.aggregate(edge2);

        final Edge aggregatedEdge = new Edge("group");
        aggregator.state(aggregatedEdge);

        // Then
        assertTrue(aggregatedEdge.getProperties().isEmpty());
    }

    @Test
    public void shouldSetStateOnElement() {
        // Given
        final Object[] state = {"state1", "state2"};
        final ElementAggregator aggregator = new ElementAggregator();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        given(function.state()).willReturn(state);

        aggregator.addFunction(functionContext1);

        final Edge edge = new Edge("group");

        // When
        aggregator.state(edge);

        // Then
        final ArgumentCaptor<PropertiesTuple> propertiesTupleCaptor = ArgumentCaptor.forClass(PropertiesTuple.class);
        verify(functionContext1).project(propertiesTupleCaptor.capture(), Mockito.eq(state));
        assertEquals(edge.getProperties(), propertiesTupleCaptor.getValue().getProperties());
    }

    @Test
    public void shouldSetStateOnProperties() {
        // Given
        final Object[] state = {"state1", "state2"};
        final ElementAggregator aggregator = new ElementAggregator();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        given(function.state()).willReturn(state);

        aggregator.addFunction(functionContext1);

        final Properties properties = new Properties();

        // When
        aggregator.state(properties);

        // Then
        final ArgumentCaptor<PropertiesTuple> propertiesTupleCaptor = ArgumentCaptor.forClass(PropertiesTuple.class);
        verify(functionContext1).project(propertiesTupleCaptor.capture(), Mockito.eq(state));
        assertSame(properties, propertiesTupleCaptor.getValue().getProperties());
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final String reference1 = "reference1";
        final ElementAggregator aggregator = new ElementAggregator();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final AggregateFunction clonedFunction = mock(AggregateFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        given(functionContext1.getSelection()).willReturn(Collections.singletonList(reference1));
        given(function.statelessClone()).willReturn(clonedFunction);

        aggregator.addFunction(functionContext1);

        // When
        final ElementAggregator clone = aggregator.clone();

        // Then
        assertNotSame(aggregator, clone);
        assertEquals(1, clone.getFunctions().size());
        final PassThroughFunctionContext<String, AggregateFunction> resultClonedFunction = clone.getFunctions().get(0);
        assertEquals(1, resultClonedFunction.getSelection().size());
        assertEquals(reference1, resultClonedFunction.getSelection().get(0));
        assertNotSame(functionContext1, resultClonedFunction);
        assertNotSame(function, resultClonedFunction.getFunction());
        assertSame(clonedFunction, resultClonedFunction.getFunction());
    }

    @Test
    public void shouldBuildAggregator() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String property3a = "property 3a";
        final String property3b = "property 3b";
        final String property5 = "property 5";

        final AggregateFunction func1 = mock(AggregateFunction.class);
        final AggregateFunction func3 = mock(AggregateFunction.class);
        final AggregateFunction func4 = mock(AggregateFunction.class);
        final AggregateFunction func5 = mock(AggregateFunction.class);

        // When - check you can build the selection/function in any order,
        // although normally it will be done - select then execute.
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(property1)
                .execute(func1)
                .select(property2)
                .select(property3a, property3b)
                .execute(func3)
                .execute(func4)
                .execute(func5)
                .select(property5)
                .build();

        // Then
        int i = 0;
        PassThroughFunctionContext<String, AggregateFunction> context = aggregator.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(property1, context.getSelection().get(0));
        assertSame(func1, context.getFunction());

        context = aggregator.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(property2, context.getSelection().get(0));

        context = aggregator.getFunctions().get(i++);
        assertEquals(2, context.getSelection().size());
        assertEquals(property3a, context.getSelection().get(0));
        assertEquals(property3b, context.getSelection().get(1));
        assertSame(func3, context.getFunction());

        context = aggregator.getFunctions().get(i++);
        assertSame(func4, context.getFunction());

        context = aggregator.getFunctions().get(i++);
        assertSame(func5, context.getFunction());
        assertEquals(1, context.getSelection().size());
        assertEquals(property5, context.getSelection().get(0));

        assertEquals(i, aggregator.getFunctions().size());
    }
}
