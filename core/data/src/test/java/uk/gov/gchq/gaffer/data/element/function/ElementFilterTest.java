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
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ElementFilterTest {

    @Test
    public void shouldWrapElementInElementTupleAndCallSuper() {
        // Given
        final String reference = "reference1";
        final String value = "value";
        final ElementFilter filter = new ElementFilter();
        final ConsumerFunctionContext<String, FilterFunction> functionContext1 = mock(ConsumerFunctionContext.class);
        final FilterFunction function = mock(FilterFunction.class);
        given(functionContext1.getFunction()).willReturn(function);

        filter.addFunction(functionContext1);

        final Element element = mock(Element.class);
        given(element.getProperty(reference)).willReturn(value);

        final ArgumentCaptor<ElementTuple> elementTupleCaptor = ArgumentCaptor.forClass(ElementTuple.class);
        given(functionContext1.select(elementTupleCaptor.capture())).willReturn(new Object[]{value});

        // When
        filter.filter(element);

        // Then
        assertSame(element, elementTupleCaptor.getValue().getElement());
        verify(functionContext1).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function).isValid(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);
    }

    @Test
    public void shouldCloneFilter() {
        // Given
        final String reference1 = "reference1";
        final ElementFilter filter = new ElementFilter();
        final ConsumerFunctionContext<String, FilterFunction> functionContext1 = mock(ConsumerFunctionContext.class);
        final FilterFunction function = mock(FilterFunction.class);
        final FilterFunction clonedFunction = mock(FilterFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        given(functionContext1.getSelection()).willReturn(Collections.singletonList(reference1));
        given(function.statelessClone()).willReturn(clonedFunction);

        filter.addFunction(functionContext1);

        // When
        final ElementFilter clone = filter.clone();

        // Then
        assertNotSame(filter, clone);
        assertEquals(1, clone.getFunctions().size());
        final ConsumerFunctionContext<String, FilterFunction> resultClonedFunction = clone.getFunctions().get(0);
        assertEquals(1, resultClonedFunction.getSelection().size());
        assertEquals(reference1, resultClonedFunction.getSelection().get(0));
        assertNotSame(functionContext1, resultClonedFunction);
        assertNotSame(function, resultClonedFunction.getFunction());
        assertSame(clonedFunction, resultClonedFunction.getFunction());
    }

    @Test
    public void shouldBuildFilter() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String property3a = "property 3a";
        final String property3b = "property 3b";
        final IdentifierType identifierType5 = IdentifierType.VERTEX;

        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func3 = mock(FilterFunction.class);
        final FilterFunction func4 = mock(FilterFunction.class);
        final FilterFunction func5 = mock(FilterFunction.class);

        // When - check you can build the selection/function in any order,
        // although normally it will be done - select then execute.
        final ElementFilter filter = new ElementFilter.Builder()
                .select(property1)
                .execute(func1)
                .select(property2)
                .select(property3a, property3b)
                .execute(func3)
                .execute(func4)
                .execute(func5)
                .select(identifierType5.name())
                .build();

        // Then
        int i = 0;
        ConsumerFunctionContext<String, FilterFunction> context = filter.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(property1, context.getSelection().get(0));
        assertSame(func1, context.getFunction());

        context = filter.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(property2, context.getSelection().get(0));

        context = filter.getFunctions().get(i++);
        assertEquals(2, context.getSelection().size());
        assertEquals(property3a, context.getSelection().get(0));
        assertEquals(property3b, context.getSelection().get(1));
        assertSame(func3, context.getFunction());

        context = filter.getFunctions().get(i++);
        assertSame(func4, context.getFunction());

        context = filter.getFunctions().get(i++);
        assertSame(func5, context.getFunction());
        assertEquals(1, context.getSelection().size());
        assertEquals(identifierType5.name(), context.getSelection().get(0));

        assertEquals(i, filter.getFunctions().size());
    }
}
