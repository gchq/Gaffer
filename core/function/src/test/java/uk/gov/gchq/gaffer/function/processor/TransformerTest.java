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
import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransformerTest {
    private TransformFunction function1;
    private TransformFunction function2;

    @Before
    public void setup() {
        function1 = mock(TransformFunction.class);
        function2 = mock(TransformFunction.class);
    }

    @Test
    public void shouldNotDoNothingIfTransformCalledWithNoFunctions() {
        // Given
        final Transformer transformer = new Transformer();

        // When
        transformer.transform(null);

        // Then - no null pointer exceptions should be thrown
    }

    @Test
    public void shouldTransformWith1FunctionAnd1Selection() {
        // Given
        final String reference = "reference1";
        final String value = "property value";
        final Transformer<String> transformer = new Transformer<>();
        final ConsumerProducerFunctionContext<String, TransformFunction> functionContext1 = mock(ConsumerProducerFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);

        transformer.addFunction(functionContext1);

        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference)).willReturn(value);
        given(functionContext1.select(tuple)).willReturn(new String[]{value});

        // When
        transformer.transform(tuple);

        // Then
        verify(functionContext1).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function1).transform(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);
    }

    @Test
    public void shouldTransformWith2FunctionsAnd2Selections() {
        // Given
        final String reference1 = "name 1";
        final String reference2 = "name 2";
        final String value1 = "property value1";
        final String value2 = "property value2";

        final Transformer<String> transformer = new Transformer<>();

        final ConsumerProducerFunctionContext<String, TransformFunction> functionContext1 = mock(ConsumerProducerFunctionContext.class);
        given(functionContext1.getFunction()).willReturn(function1);
        transformer.addFunction(functionContext1);

        final ConsumerProducerFunctionContext<String, TransformFunction> functionContext2 = mock(ConsumerProducerFunctionContext.class);
        given(functionContext2.getFunction()).willReturn(function2);
        transformer.addFunction(functionContext2);

        final Tuple<String> tuple = mock(Tuple.class);
        given(tuple.get(reference1)).willReturn(value1);
        given(tuple.get(reference2)).willReturn(value2);

        given(functionContext1.select(tuple)).willReturn(new String[]{value1, value2});
        given(functionContext2.select(tuple)).willReturn(new String[]{value2});

        // When
        transformer.transform(tuple);

        // Then
        verify(functionContext1).getFunction();
        verify(functionContext2).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor1 = ArgumentCaptor.forClass(Object[].class);
        verify(function1).transform(argumentCaptor1.capture());
        assertEquals(value1, argumentCaptor1.getValue()[0]);
        assertEquals(value2, argumentCaptor1.getValue()[1]);

        final ArgumentCaptor<Object[]> argumentCaptor3 = ArgumentCaptor.forClass(Object[].class);
        verify(function2).transform(argumentCaptor3.capture());
        assertEquals(value2, argumentCaptor3.getValue()[0]);
    }

    @Test
    public void shouldCloneTransformer() {
        // Given
        final String reference1 = "reference1";
        final String reference2 = "reference2";
        final Transformer<String> transformer = new Transformer<>();
        final ConsumerProducerFunctionContext<String, TransformFunction> functionContext1 = mock(ConsumerProducerFunctionContext.class);
        final TransformFunction function = mock(TransformFunction.class);
        final TransformFunction clonedFunction = mock(TransformFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        given(functionContext1.getSelection()).willReturn(Collections.singletonList(reference1));
        given(functionContext1.getProjection()).willReturn(Collections.singletonList(reference2));
        given(function.statelessClone()).willReturn(clonedFunction);

        transformer.addFunction(functionContext1);

        // When
        final Transformer<String> clone = transformer.clone();

        // Then
        assertNotSame(transformer, clone);
        assertEquals(1, clone.getFunctions().size());
        final ConsumerProducerFunctionContext<String, TransformFunction> resultClonedFunction = clone.getFunctions().get(0);
        assertEquals(1, resultClonedFunction.getSelection().size());
        assertEquals(reference1, resultClonedFunction.getSelection().get(0));
        assertEquals(1, resultClonedFunction.getProjection().size());
        assertEquals(reference2, resultClonedFunction.getProjection().get(0));
        assertNotSame(functionContext1, resultClonedFunction);
        assertNotSame(function, resultClonedFunction.getFunction());
        assertSame(clonedFunction, resultClonedFunction.getFunction());
    }

    @Test
    public void shouldBuildTransformer() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final String reference3a = "reference 3a";
        final String reference3b = "reference 3b";
        final String reference5 = "reference 5";

        final String reference1Proj = "reference 1 proj";
        final String reference2Proj = "reference 2 proj";
        final String reference3aProj = "reference 3a proj";
        final String reference3bProj = "reference 3b proj";
        final String reference5Proj = "reference 5 proj";

        final TransformFunction func1 = mock(TransformFunction.class);
        final TransformFunction func3 = mock(TransformFunction.class);
        final TransformFunction func4 = mock(TransformFunction.class);
        final TransformFunction func5 = mock(TransformFunction.class);

        // When - check you can build the selection/function/projections in any order,
        // although normally it will be done - select, execute then project.
        final Transformer<String> transformer = new Transformer.Builder<String>()
                .select(reference1)
                .execute(func1)
                .project(reference1Proj)
                .select(reference2)
                .project(reference2Proj)
                .project(new String[]{reference3aProj, reference3bProj})
                .select(new String[]{reference3a, reference3b})
                .execute(func3)
                .execute(func4)
                .execute(func5)
                .project(reference5Proj)
                .select(reference5)
                .build();

        // Then
        int i = 0;
        ConsumerProducerFunctionContext<String, TransformFunction> context = transformer.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(reference1, context.getSelection().get(0));
        assertSame(func1, context.getFunction());
        assertEquals(1, context.getProjection().size());
        assertEquals(reference1Proj, context.getProjection().get(0));

        context = transformer.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(reference2, context.getSelection().get(0));
        assertEquals(1, context.getProjection().size());
        assertEquals(reference2Proj, context.getProjection().get(0));

        context = transformer.getFunctions().get(i++);
        assertEquals(2, context.getSelection().size());
        assertEquals(reference3a, context.getSelection().get(0));
        assertEquals(reference3b, context.getSelection().get(1));
        assertSame(func3, context.getFunction());
        assertEquals(2, context.getProjection().size());
        assertEquals(reference3aProj, context.getProjection().get(0));
        assertEquals(reference3bProj, context.getProjection().get(1));

        context = transformer.getFunctions().get(i++);
        assertSame(func4, context.getFunction());

        context = transformer.getFunctions().get(i++);
        assertSame(func5, context.getFunction());
        assertEquals(1, context.getSelection().size());
        assertEquals(reference5, context.getSelection().get(0));
        assertEquals(1, context.getProjection().size());
        assertEquals(reference5Proj, context.getProjection().get(0));

        assertEquals(i, transformer.getFunctions().size());
    }
}
