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
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.impl.function.Identity;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ElementTransformerTest {
    @Test
    public void shouldTransformElementUsingMockFunction() {
        // Given
        final String selection = "reference1";
        final String projection = "reference1";
        final Integer valueResult = 3;

        final Function<String, Integer> function = mock(Function.class);
        given(function.apply("value1")).willReturn(valueResult);

        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(selection)
                .execute(function)
                .project(projection)
                .build();

        final Edge edge = new Edge.Builder()
                .property(selection, "value1")
                .build();

        // When
        final Element result = transformer.apply(edge);

        // Then
        assertEquals(valueResult, result.getProperty(projection));
    }

    @Test
    public void shouldTransformElementUsingIdentityFunction() {
        // Given
        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select("prop1")
                .execute(new Identity())
                .project("prop3")
                .build();

        final Entity element = new Entity.Builder()
                .group("test")
                .property("prop1", "value")
                .property("prop2", 1)
                .build();

        // When
        final Element result = transformer.apply(element);

        // Then
        assertEquals(element.getProperty("prop1"), result.getProperty("prop3"));
    }

    @Test
    public void shouldTransformElementUsingInlineFunction() {
        // Given
        final Function<String, Integer> function = String::length;
        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select("prop1")
                .execute(function)
                .project("prop3")
                .build();

        final Entity element = new Entity.Builder()
                .group("test")
                .property("prop1", "value")
                .property("prop2", 1)
                .build();

        // When
        final Element result = transformer.apply(element);

        // Then
        assertEquals("prop1".length(), result.getProperty("prop3"));
    }

    @Test
    public void shouldBuildTransformer() {
        // Given
        final String property1 = "property 1";
        final String property2a = "property 2a";
        final String property2b = "property 2b";
        final IdentifierType identifier3 = IdentifierType.SOURCE;

        final String property1Proj = "property 1 proj";
        final String property2aProj = "property 2a proj";
        final String property2bProj = "property 2b proj";
        final IdentifierType identifier3Proj = IdentifierType.DESTINATION;

        final Function func1 = mock(Function.class);
        final Function func2 = mock(Function.class);
        final Function func3 = mock(Function.class);

        // When - check you can build the selection/function/projections in any order,
        // although normally it will be done - select, execute then project.
        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(property1)
                .execute(func1)
                .project(property1Proj)
                .select(property2a, property2b)
                .execute(func2)
                .project(property2aProj, property2bProj)
                .select(identifier3.name())
                .execute(func3)
                .project(identifier3Proj.name())
                .build();

        // Then
        int i = 0;
        TupleAdaptedFunction<String, ?, ?> context = transformer.getComponents().get(i++);
        assertEquals(1, context.getSelection().length);
        assertEquals(property1, context.getSelection()[0]);
        assertSame(func1, context.getFunction());
        assertEquals(1, context.getProjection().length);
        assertEquals(property1Proj, context.getProjection()[0]);

        context = transformer.getComponents().get(i++);
        assertEquals(2, context.getSelection().length);
        assertEquals(property2a, context.getSelection()[0]);
        assertEquals(property2b, context.getSelection()[1]);
        assertSame(func2, context.getFunction());
        assertEquals(2, context.getProjection().length);
        assertEquals(property2aProj, context.getProjection()[0]);
        assertEquals(property2bProj, context.getProjection()[1]);

        context = transformer.getComponents().get(i++);
        assertSame(func3, context.getFunction());
        assertEquals(1, context.getSelection().length);
        assertEquals(identifier3.name(), context.getSelection()[0]);
        assertEquals(1, context.getProjection().length);
        assertEquals(identifier3Proj.name(), context.getProjection()[0]);

        assertEquals(i, transformer.getComponents().size());
    }
}
