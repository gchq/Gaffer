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
import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ElementTransformerTest {

    @Test
    public void shouldWrapElementInElementTupleAndCallSuper() {
        // Given
        final String reference = "reference1";
        final String value = "value";
        final ElementTransformer transformer = new ElementTransformer();
        final ConsumerProducerFunctionContext<String, TransformFunction> functionContext1 = mock(ConsumerProducerFunctionContext.class);
        final TransformFunction function = mock(TransformFunction.class);
        given(functionContext1.getFunction()).willReturn(function);

        transformer.addFunction(functionContext1);

        final Element element = mock(Element.class);
        given(element.getProperty(reference)).willReturn(value);

        final ArgumentCaptor<ElementTuple> elementTupleCaptor = ArgumentCaptor.forClass(ElementTuple.class);
        given(functionContext1.select(elementTupleCaptor.capture())).willReturn(new Object[]{value});

        // When
        transformer.transform(element);

        // Then
        assertSame(element, elementTupleCaptor.getValue().getElement());
        verify(functionContext1).getFunction();

        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(function).transform(argumentCaptor.capture());
        assertEquals(value, argumentCaptor.getValue()[0]);
    }

    @Test
    public void shouldCloneTransformer() {
        // Given
        final String reference1 = "reference1";
        final String reference2 = "reference2";
        final ElementTransformer transformer = new ElementTransformer();
        final ConsumerProducerFunctionContext<String, TransformFunction> functionContext1 = mock(ConsumerProducerFunctionContext.class);
        final TransformFunction function = mock(TransformFunction.class);
        final TransformFunction clonedFunction = mock(TransformFunction.class);
        given(functionContext1.getFunction()).willReturn(function);
        given(functionContext1.getSelection()).willReturn(Collections.singletonList(reference1));
        given(functionContext1.getProjection()).willReturn(Collections.singletonList(reference2));
        given(function.statelessClone()).willReturn(clonedFunction);

        transformer.addFunction(functionContext1);

        // When
        final ElementTransformer clone = transformer.clone();

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
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String property3a = "property 3a";
        final String property3b = "property 3b";
        final IdentifierType identifier5 = IdentifierType.SOURCE;

        final String property1Proj = "property 1 proj";
        final String property2Proj = "property 2 proj";
        final String property3aProj = "property 3a proj";
        final String property3bProj = "property 3b proj";
        final IdentifierType identifier5Proj = IdentifierType.DESTINATION;

        final TransformFunction func1 = mock(TransformFunction.class);
        final TransformFunction func3 = mock(TransformFunction.class);
        final TransformFunction func4 = mock(TransformFunction.class);
        final TransformFunction func5 = mock(TransformFunction.class);

        // When - check you can build the selection/function/projections in any order,
        // although normally it will be done - select, execute then project.
        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(property1)
                .execute(func1)
                .project(property1Proj)
                .select(property2)
                .project(property2Proj)
                .project(property3aProj, property3bProj)
                .select(property3a, property3b)
                .execute(func3)
                .execute(func4)
                .execute(func5)
                .project(identifier5Proj.name())
                .select(identifier5.name())
                .build();

        // Then
        int i = 0;
        ConsumerProducerFunctionContext<String, TransformFunction> context = transformer.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(property1, context.getSelection().get(0));
        assertSame(func1, context.getFunction());
        assertEquals(1, context.getProjection().size());
        assertEquals(property1Proj, context.getProjection().get(0));

        context = transformer.getFunctions().get(i++);
        assertEquals(1, context.getSelection().size());
        assertEquals(property2, context.getSelection().get(0));
        assertEquals(1, context.getProjection().size());
        assertEquals(property2Proj, context.getProjection().get(0));

        context = transformer.getFunctions().get(i++);
        assertEquals(2, context.getSelection().size());
        assertEquals(property3a, context.getSelection().get(0));
        assertEquals(property3b, context.getSelection().get(1));
        assertSame(func3, context.getFunction());
        assertEquals(2, context.getProjection().size());
        assertEquals(property3aProj, context.getProjection().get(0));
        assertEquals(property3bProj, context.getProjection().get(1));

        context = transformer.getFunctions().get(i++);
        assertSame(func4, context.getFunction());

        context = transformer.getFunctions().get(i++);
        assertSame(func5, context.getFunction());
        assertEquals(1, context.getSelection().size());
        assertEquals(identifier5.name(), context.getSelection().get(0));
        assertEquals(1, context.getProjection().size());
        assertEquals(identifier5Proj.name(), context.getProjection().get(0));

        assertEquals(i, transformer.getFunctions().size());
    }
}
