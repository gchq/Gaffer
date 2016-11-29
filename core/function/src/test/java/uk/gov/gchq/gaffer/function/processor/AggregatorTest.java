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

package uk.gov.gchq.gaffer.function.processor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import uk.gov.gchq.gaffer.function.context.PassThroughFunctionContext;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AggregatorTest {
    private AggregateFunction function1;
    private AggregateFunction function2;

    @Before
    public void setup() {
        function1 = mock(AggregateFunction.class);
        function2 = mock(AggregateFunction.class);
    }

    @Test
    public void shouldNotDoNothingIfInitCalledWithNoFunctions() {
        // Given
        final Aggregator<String> aggregator = new Aggregator<>();

        // When
        aggregator.initFunctions();

        // Then - no null pointer exceptions should be thrown
    }

    @Test
    public void shouldAddAndInitAggregateFunction() {
        // Given
        final Aggregator<String> aggregator = new Aggregator<>();

        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function1 = mock(AggregateFunction.class);
        given(functionContext1.getFunction()).willReturn(function1);

        // When
        aggregator.addFunction(functionContext1);
        aggregator.initFunctions();

        // Then
        verify(functionContext1).getFunction();
        verify(function1).init();
    }

    @Test
    public void shouldNotDoNothingIfAggregateCalledWithNoFunctions() {
        // Given
        final Aggregator<String> aggregator = new Aggregator<>();

        // When
        aggregator.aggregate(null);

        // Then - no null pointer exceptions should be thrown
    }

    @Test
    public void shouldAggregateWith1FunctionAnd1Selection() {
        // Given
        final String reference = "reference1";
        final String value = "value";
        final Aggregator<String> aggregator = new Aggregator<>();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        final List<String> references = Collections.singletonList(reference);
        given(functionContext1.getSelection()).willReturn(references);

        aggregator.addFunction(functionContext1);

        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference)).willReturn(value);
        given(functionContext1.select(tuple)).willReturn(new String[]{value});

        // When
        aggregator.aggregate(tuple);

        // Then
        verify(functionContext1, times(2)).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function1).aggregate(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);
    }

    @Test
    public void shouldAggregateWith2FunctionsAnd2Selections() {
        // Given
        final String reference1 = "reference1";
        final String reference2 = "reference2";
        final String value1 = "value1";
        final String value2 = "value2";
        final Aggregator<String> aggregator = new Aggregator<>();

        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        aggregator.addFunction(functionContext1);

        final PassThroughFunctionContext<String, AggregateFunction> functionContext2 = mock(PassThroughFunctionContext.class);
        given(functionContext2.getFunction()).willReturn(function2);
        aggregator.addFunction(functionContext2);


        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference1)).willReturn(value1);
        given(tuple.get(reference2)).willReturn(value2);

        given(functionContext1.select(tuple)).willReturn(new String[]{value1, value2});
        given(functionContext2.select(tuple)).willReturn(new String[]{value2});

        // When
        aggregator.aggregate(tuple);

        // Then
        verify(functionContext1, times(2)).getFunction();
        verify(functionContext2, times(2)).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor1 = ArgumentCaptor.forClass(Object[].class);
        verify(function1).aggregate(argumentCaptor1.capture());
        assertEquals(value1, argumentCaptor1.getValue()[0]);
        assertEquals(value2, argumentCaptor1.getValue()[1]);

        final ArgumentCaptor<Object[]> argumentCaptor3 = ArgumentCaptor.forClass(Object[].class);
        verify(function2).aggregate(argumentCaptor3.capture());
        assertEquals(value2, argumentCaptor3.getValue()[0]);
    }

    @Test
    public void shouldNotCallAggregateFunctionIfSelectionContainsAllNullValues() {
        // Given
        final String reference1 = "reference1";
        final String reference2 = "reference2";
        final String value1 = null;
        final String value2 = null;
        final Aggregator<String> aggregator = new Aggregator<>();

        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        aggregator.addFunction(functionContext1);

        final PassThroughFunctionContext<String, AggregateFunction> functionContext2 = mock(PassThroughFunctionContext.class);
        given(functionContext2.getFunction()).willReturn(function2);
        aggregator.addFunction(functionContext2);


        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference1)).willReturn(value1);
        given(tuple.get(reference2)).willReturn(value2);

        given(functionContext1.select(tuple)).willReturn(new String[]{value1, value2});
        given(functionContext2.select(tuple)).willReturn(new String[]{value2});

        // When
        aggregator.aggregate(tuple);

        // Then
        verify(functionContext1).getFunction();
        verify(functionContext2).getFunction();
        verify(function1, never()).aggregate(Mockito.any(Object[].class));
        verify(function2, never()).aggregate(Mockito.any(Object[].class));
    }


    @Test
    public void shouldNotDoNothingIfStateCalledWithNoFunctions() {
        // Given
        final Aggregator<String> aggregator = new Aggregator<>();

        // When
        aggregator.state(null);

        // Then - no null pointer exceptions should be thrown
    }

    @Test
    public void shouldSetStateOnTuple() {
        // Given
        final Object[] state = {"state1", "state2"};
        final Aggregator<String> aggregator = new Aggregator<>();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        final Tuple<String> tuple = mock(Tuple.class);

        given(functionContext1.getFunction()).willReturn(function1);
        given(function1.state()).willReturn(state);
        aggregator.addFunction(functionContext1);

        // When
        aggregator.state(tuple);

        // Then
        verify(functionContext1).project(tuple, state);
    }

    @Test
    public void shouldCloneAggregator() {
        // Given
        final String reference1 = "reference1";
        final Aggregator<String> aggregator = new Aggregator<>();
        final PassThroughFunctionContext<String, AggregateFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        given(functionContext1.getSelection()).willReturn(Collections.singletonList(reference1));
        given(function1.statelessClone()).willReturn(function2);

        aggregator.addFunction(functionContext1);

        // When
        final Aggregator<String> clone = aggregator.clone();

        // Then
        assertNotSame(aggregator, clone);
        assertEquals(1, clone.getFunctions().size());
        final PassThroughFunctionContext<String, AggregateFunction> resultClonedFunction = clone.getFunctions().get(0);
        assertEquals(1, resultClonedFunction.getSelection().size());
        assertEquals(reference1, resultClonedFunction.getSelection().get(0));
        assertNotSame(functionContext1, resultClonedFunction);
        assertNotSame(function1, resultClonedFunction.getFunction());
        assertSame(function2, resultClonedFunction.getFunction());
    }

    @Test
    public void shouldBuildAggregator() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final String reference3a = "reference 3a";
        final String reference3b = "reference 3b";
        final String reference5 = "reference 5";

        final AggregateFunction func1 = mock(AggregateFunction.class);
        final AggregateFunction func3 = mock(AggregateFunction.class);
        final AggregateFunction func4 = mock(AggregateFunction.class);
        final AggregateFunction func5 = mock(AggregateFunction.class);

        // When - check you can build the selection/function in any order,
        // although normally it will be done - select then aggregate.
        final Aggregator<String> aggregator = new Aggregator.Builder<String>()
                .select(reference1)
                .execute(func1)
                .select(reference2)
                .select(reference3a, reference3b)
                .execute(func3)
                .execute(func4)
                .execute(func5)
                .select(reference5)
                .build();

        // Then
        int i = 0;
        PassThroughFunctionContext<String, AggregateFunction> context = aggregator.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(reference1, context.getSelection().get(0));
        assertSame(func1, context.getFunction());

        context = aggregator.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(reference2, context.getSelection().get(0));

        context = aggregator.getFunctions().get(i++);
        assertEquals(2, context.getSelection().size());
        assertEquals(reference3a, context.getSelection().get(0));
        assertEquals(reference3b, context.getSelection().get(1));
        assertSame(func3, context.getFunction());

        context = aggregator.getFunctions().get(i++);
        assertSame(func4, context.getFunction());

        context = aggregator.getFunctions().get(i++);
        assertSame(func5, context.getFunction());
        assertEquals(1, context.getSelection().size());
        assertEquals(reference5, context.getSelection().get(0));

        assertEquals(i, aggregator.getFunctions().size());
    }
}
