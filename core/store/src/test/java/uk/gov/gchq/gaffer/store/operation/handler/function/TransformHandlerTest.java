/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.function;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.koryphe.impl.function.Identity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class TransformHandlerTest {
    private List<Element> input;
    private List<Element> expected;
    private Store store;
    private Context context;
    private TransformHandler handler;

    @Before
    public void setup() {
        input = new ArrayList<>();
        expected = new ArrayList<>();
        store = mock(Store.class);
        context = new Context();
        handler = new TransformHandler();

    }

    @Test
    public void shouldTransformElementsUsingMockFunction() throws OperationException {
        // Given
        final Function<String, Integer> function = mock(Function.class);
        given(function.apply(TestPropertyNames.STRING)).willReturn(6);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, TestPropertyNames.STRING)
                .build();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, TestPropertyNames.STRING)
                .property(TestPropertyNames.PROP_2, 3)
                .build();

        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function)
                .project(TestPropertyNames.PROP_3)
                .build();

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_3, 6)
                .build();

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, 6)
                .property(TestPropertyNames.PROP_2, 3)
                .build();

        final Transform transform = new Transform.Builder()
                .input(input)
                .edge(TestGroups.EDGE, transformer)
                .entity(TestGroups.ENTITY, transformer)
                .build();

        input.add(edge);
        input.add(entity);

        expected.add(expectedEdge);
        expected.add(expectedEntity);

        // When
        final Iterable<? extends Element> results = handler.doOperation(transform, context, store);
        final List<Element> resultsList = Lists.newArrayList(results);
        // Then
        boolean isSame = false;
        for (int i = 0; i < resultsList.size(); i++) {
            isSame = expected.get(i).getProperty(TestPropertyNames.PROP_3).equals(resultsList.get(i).getProperty(TestPropertyNames.PROP_3));
        }

        assertTrue(isSame);
    }

    @Test
    public void shouldTransformElementsUsingIdentityFunction() throws OperationException {
        // Given
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, TestPropertyNames.INT)
                .property(TestPropertyNames.PROP_2, TestPropertyNames.STRING)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, TestPropertyNames.INT)
                .build();

        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new Identity())
                .project(TestPropertyNames.PROP_3)
                .build();

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, TestPropertyNames.INT)
                .property(TestPropertyNames.PROP_2, TestPropertyNames.STRING)
                .build();

        final Entity expectedEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, TestPropertyNames.INT)
                .build();

        final Transform transform = new Transform.Builder()
                .input(input)
                .entity(TestGroups.ENTITY, transformer)
                .build();

        input.add(entity);
        input.add(entity1);

        expected.add(expectedEntity);
        expected.add(expectedEntity1);

        // When
        final Iterable<? extends Element> results = handler.doOperation(transform, context, store);
        final List<Element> resultsList = Lists.newArrayList(results);

        // Then
        boolean isSame = false;
        for (int i = 0; i < expected.size(); i++) {
            isSame = expected.get(i).getProperty(TestPropertyNames.PROP_3).equals(resultsList.get(i).getProperty(TestPropertyNames.PROP_3));
        }
        assertTrue(isSame);
    }

    @Test
    public void shouldTransformElementsUsingInlineFunction() throws OperationException {
        // Given
        final Function<String, Integer> function = String::length;
        final Map<String, ElementTransformer> entities = new HashMap<>();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, "value")
                .property(TestPropertyNames.PROP_2, 1)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .property(TestPropertyNames.PROP_1, "otherValue")
                .property(TestPropertyNames.PROP_2, 3)
                .build();

        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function)
                .project(TestPropertyNames.PROP_3)
                .build();

        entities.put(TestGroups.ENTITY, transformer);
        entities.put(TestGroups.ENTITY_2, transformer);

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_3, 5)
                .property(TestPropertyNames.PROP_2, 1)
                .build();

        final Entity expectedEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .property(TestPropertyNames.PROP_3, 10)
                .property(TestPropertyNames.PROP_2, 3)
                .build();

        final Transform transform = new Transform.Builder()
                .input(input)
                .entities(entities)
                .build();

        input.add(entity);
        input.add(entity1);

        expected.add(expectedEntity);
        expected.add(expectedEntity1);

        // When
        final Iterable<? extends Element> results = handler.doOperation(transform, context, store);
        final List<Element> resultsList = Lists.newArrayList(results);

        // Then
        boolean isSame = false;
        for (int i = 0; i < resultsList.size(); i++) {
            isSame = expected.get(i).getProperty(TestPropertyNames.PROP_3).equals(resultsList.get(i).getProperty(TestPropertyNames.PROP_3));
        }

        assertTrue(isSame);
    }

    @Test
    public void shouldApplyDifferentTransformersToDifferentGroups() throws OperationException {
        // Given
        final Function<String, Integer> function = String::length;
        final Function<String, String> function1 = String::toUpperCase;
        final Map<String, ElementTransformer> edges = new HashMap<>();

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_1, "testValue")
                .property(TestPropertyNames.PROP_2, 5)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .property(TestPropertyNames.PROP_2, "otherValue")
                .property(TestPropertyNames.PROP_3, 2L)
                .build();

        final ElementTransformer elementTransformer = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function)
                .project(TestPropertyNames.PROP_4)
                .build();

        final ElementTransformer elementTransformer1 = new ElementTransformer.Builder()
                .select(TestPropertyNames.PROP_2)
                .execute(function1)
                .project(TestPropertyNames.PROP_4)
                .build();

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.PROP_4, 9)
                .property(TestPropertyNames.PROP_2, 5)
                .build();

        final Edge expectedEdge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .property(TestPropertyNames.PROP_4, "OTHERVALUE")
                .property(TestPropertyNames.PROP_3, 2L)
                .build();

        edges.put(TestGroups.EDGE, elementTransformer);
        edges.put(TestGroups.EDGE_2, elementTransformer1);

        input.add(edge);
        input.add(edge1);

        expected.add(expectedEdge);
        expected.add(expectedEdge1);

        final Transform transform = new Transform.Builder()
                .input(input)
                .edges(edges)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(transform, context, store);
        final List<Element> resultsList = Lists.newArrayList(results);

        // Then
        boolean isSame = false;
        for (int i = 0; i < resultsList.size(); i++) {
            isSame = expected.get(i).getProperty(TestPropertyNames.PROP_4).equals(resultsList.get(i).getProperty(TestPropertyNames.PROP_4));
        }

        assertTrue(isSame);
    }
    // todo multiple transformers in the map for different groups
}
