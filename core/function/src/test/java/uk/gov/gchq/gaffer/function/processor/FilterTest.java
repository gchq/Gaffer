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
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.context.PassThroughFunctionContext;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FilterTest {
    private FilterFunction function1;
    private FilterFunction function2;

    @Before
    public void setup() {
        function1 = mock(FilterFunction.class);
        function2 = mock(FilterFunction.class);
    }

    @Test
    public void shouldNotDoNothingIfFilterCalledWithNoFunctions() {
        // Given
        final Filter<String> filter = new Filter<>();

        // When
        filter.filter(null);

        // Then - no null pointer exceptions should be thrown
    }

    @Test
    public void shouldFilterWith1FunctionAnd1Selection() {
        // Given
        final String reference = "reference1";
        final String value = "property value";
        final Filter<String> filter = new Filter<>();
        final ConsumerFunctionContext<String, FilterFunction> functionContext1 = mock(ConsumerFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        final List<String> references = Collections.singletonList(reference);
        given(functionContext1.getSelection()).willReturn(references);

        given(function1.isValid(new String[]{value})).willReturn(true);

        filter.addFunction(functionContext1);

        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference)).willReturn(value);

        given(functionContext1.select(tuple)).willReturn(new String[]{value});

        // When
        boolean result = filter.filter(tuple);

        // Then
        verify(functionContext1).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function1).isValid(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);

        assertTrue(result);
    }

    @Test
    public void shouldAndFilterWith2FunctionsAnd2Selections() {
        // Given
        final String reference1 = "name 1";
        final String reference2 = "name 2";
        final String value1 = "property value1";
        final String value2 = "property value2";

        final Filter<String> filter = new Filter<>();

        final ConsumerFunctionContext<String, FilterFunction> functionContext1 = mock(ConsumerFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        filter.addFunction(functionContext1);

        final ConsumerFunctionContext<String, FilterFunction> functionContext2 = mock(ConsumerFunctionContext.class);
        given(functionContext2.getFunction()).willReturn(function2);
        filter.addFunction(functionContext2);

        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference1)).willReturn(value1);
        given(tuple.get(reference2)).willReturn(value2);

        given(function1.isValid(new String[]{value1, value2})).willReturn(true);
        given(function2.isValid(new String[]{value2})).willReturn(true);

        given(functionContext1.select(tuple)).willReturn(new String[]{value1, value2});
        given(functionContext2.select(tuple)).willReturn(new String[]{value2});

        // When
        boolean result = filter.filter(tuple);

        // Then
        verify(functionContext1).getFunction();
        verify(functionContext2).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor1 = ArgumentCaptor.forClass(Object[].class);
        verify(function1).isValid(argumentCaptor1.capture());
        assertEquals(value1, argumentCaptor1.getValue()[0]);
        assertEquals(value2, argumentCaptor1.getValue()[1]);

        final ArgumentCaptor<Object[]> argumentCaptor3 = ArgumentCaptor.forClass(Object[].class);
        verify(function2).isValid(argumentCaptor3.capture());
        assertEquals(value2, argumentCaptor3.getValue()[0]);

        assertTrue(result);
    }

    @Test
    public void shouldAndFilterWith2FunctionsAnd2SelectionsFalseResult() {
        // Given
        final String reference1 = "name 1";
        final String reference2 = "name 2";
        final String value1 = "property value1";
        final String value2 = "property value2";

        final Filter<String> filter = new Filter<>();

        final ConsumerFunctionContext<String, FilterFunction> functionContext1 = mock(ConsumerFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        filter.addFunction(functionContext1);

        final ConsumerFunctionContext<String, FilterFunction> functionContext2 = mock(ConsumerFunctionContext.class);
        given(functionContext2.getFunction()).willReturn(function2);
        filter.addFunction(functionContext2);

        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference1)).willReturn(value1);
        given(tuple.get(reference2)).willReturn(value2);

        given(function1.isValid(new String[]{value1, value2})).willReturn(true);
        given(function2.isValid(new String[]{value2})).willReturn(false);

        given(functionContext1.select(tuple)).willReturn(new String[]{value1, value2});
        given(functionContext2.select(tuple)).willReturn(new String[]{value2});

        // When
        boolean result = filter.filter(tuple);

        // Then
        verify(functionContext1).getFunction();
        verify(functionContext2).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor1 = ArgumentCaptor.forClass(Object[].class);
        verify(function1).isValid(argumentCaptor1.capture());
        assertEquals(value1, argumentCaptor1.getValue()[0]);
        assertEquals(value2, argumentCaptor1.getValue()[1]);

        final ArgumentCaptor<Object[]> argumentCaptor3 = ArgumentCaptor.forClass(Object[].class);
        verify(function2).isValid(argumentCaptor3.capture());
        assertEquals(value2, argumentCaptor3.getValue()[0]);

        assertFalse(result);
    }

    @Test
    public void shouldCloneFilter() {
        // Given
        final String reference1 = "reference1";
        final Filter<String> filter = new Filter<>();
        final ConsumerFunctionContext<String, FilterFunction> functionContext1 = mock(PassThroughFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        given(functionContext1.getSelection()).willReturn(Collections.singletonList(reference1));
        given(function1.statelessClone()).willReturn(function2);

        filter.addFunction(functionContext1);

        // When
        final Filter<String> clone = filter.clone();

        // Then
        assertNotSame(filter, clone);
        assertEquals(1, clone.getFunctions().size());
        final ConsumerFunctionContext<String, FilterFunction> resultClonedFunction = clone.getFunctions().get(0);
        assertEquals(1, resultClonedFunction.getSelection().size());
        assertEquals(reference1, resultClonedFunction.getSelection().get(0));
        assertNotSame(functionContext1, resultClonedFunction);
        assertNotSame(function1, resultClonedFunction.getFunction());
        assertSame(function2, resultClonedFunction.getFunction());
    }

    @Test
    public void shouldBuildFilter() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final String reference3a = "reference 3a";
        final String reference3b = "reference 3b";
        final String reference5 = "reference 5";

        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func3 = mock(FilterFunction.class);
        final FilterFunction func4 = mock(FilterFunction.class);
        final FilterFunction func5 = mock(FilterFunction.class);

        // When - check you can build the selection/function in any order,
        // although normally it will be done - select then execute.
        final Filter<String> filter = new Filter.Builder<String>()
                .select(reference1)
                .execute(func1)
                .select(reference2)
                .select(new String[]{reference3a, reference3b})
                .execute(func3)
                .execute(func4)
                .execute(func5)
                .select(reference5)
                .build();

        // Then
        int i = 0;
        ConsumerFunctionContext<String, FilterFunction> context = filter.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(reference1, context.getSelection().get(0));
        assertSame(func1, context.getFunction());

        context = filter.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(reference2, context.getSelection().get(0));

        context = filter.getFunctions().get(i++);
        assertEquals(2, context.getSelection().size());
        assertEquals(reference3a, context.getSelection().get(0));
        assertEquals(reference3b, context.getSelection().get(1));
        assertSame(func3, context.getFunction());

        context = filter.getFunctions().get(i++);
        assertSame(func4, context.getFunction());

        context = filter.getFunctions().get(i++);
        assertSame(func5, context.getFunction());
        assertEquals(1, context.getSelection().size());
        assertEquals(reference5, context.getSelection().get(0));

        assertEquals(i, filter.getFunctions().size());
    }
}
