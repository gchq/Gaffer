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
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FilterHandlerTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
            .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition())
            .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition())
            .build();

    private List<Element> input;
    private List<Element> expected;
    private Store store;
    private Context context;
    private FilterHandler handler;

    @Before
    public void setup() {
        input = new ArrayList<>();
        expected = new ArrayList<>();
        store = mock(Store.class);
        context = new Context();
        handler = new FilterHandler();

    }

    @Test
    public void shouldFilterByGroup() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Edge edge3 = new Edge.Builder()
                .group(TestGroups.EDGE_3)
                .source("junctionC")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(edge3);

        expected.add(edge1);

        final Filter filter = new Filter.Builder()
                .input(input)
                .edge(TestGroups.EDGE_2)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(expected, resultsList);
    }

    @Test
    public void shouldFilterInputBasedOnGroupAndCount() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Edge edge3 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionC")
                .dest("junctionD")
                .directed(true)
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(edge3);

        expected.add(edge);
        expected.add(edge2);

        final Filter filter = new Filter.Builder()
                .input(input)
                .edge(TestGroups.EDGE, new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(1L))
                        .build())
                .build();

        // When
        final Iterable<? extends Element> result = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultList = Streams.toStream(result).collect(Collectors.toList());
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldReturnAllValuesWithNullElementFilters() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);

        expected.add(edge);
        expected.add(edge1);
        expected.add(edge2);

        final Filter filter = new Filter.Builder()
                .input(input)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(expected, resultsList);
    }

    @Test
    public void shouldApplyGlobalFilterAndReturnOnlySpecifiedEdges() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(entity);
        input.add(entity1);

        expected.add(edge2);

        final Filter filter = new Filter.Builder()
                .input(input)
                .globalElements(new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(2L))
                        .build())
                .edge(TestGroups.EDGE_2)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(expected, resultsList);
    }

    @Test
    public void shouldFilterEntitiesAndEdges() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(entity);
        input.add(entity1);

        expected.add(edge2);
        expected.add(entity);
        expected.add(entity1);

        final Filter filter = new Filter.Builder()
                .input(input)
                .globalElements(new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(2L))
                        .build())
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(expected, resultsList);
    }

    @Test
    public void shouldHandleComplexFiltering() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Entity entity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .property(TestPropertyNames.COUNT, 6L)
                .build();

        final Filter filter = new Filter.Builder()
                .input(input)
                .globalElements(new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(1L))
                        .build())
                .edge(TestGroups.EDGE, new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(2L))
                        .build())
                .entity(TestGroups.ENTITY_2)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(entity);
        input.add(entity1);
        input.add(entity2);

        expected.add(edge2);
        expected.add(entity1);

        // When
        final Iterable<? extends Element> results = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(expected, resultsList);
    }

    @Test
    public void shouldThrowErrorForNullInput() {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Filter filter = new Filter.Builder()
                .globalElements(new ElementFilter())
                .build();

        // When / Then
        try {
            handler.doOperation(filter, context, store);
            fail("Exception expected");
        } catch (OperationException e) {
            assertEquals("Filter operation has null iterable of elements", e.getMessage());
        }
    }

    @Test
    public void shouldReturnNoResultsWhenGlobalElementsFails() throws OperationException {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionB")
                .dest("junctionA")
                .directed(true)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.COUNT, 3L)
                .build();

        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .property(TestPropertyNames.COUNT, 4L)
                .build();

        final Entity entity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .property(TestPropertyNames.COUNT, 6L)
                .build();

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(entity);
        input.add(entity1);
        input.add(entity2);

        final Filter filter = new Filter.Builder()
                .input(input)
                .globalElements(new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(10L))
                        .build())
                .globalEdges(new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(2L))
                        .build())
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(filter, context, store);

        // Then
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(expected, resultsList);
    }

    @Test
    public void shouldFailValidationWhenSchemaElementDefinitionsAreNull() {
        // Given
        given(store.getSchema()).willReturn(new Schema());

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        input.add(edge);
        input.add(edge1);

        final Filter filter = new Filter.Builder()
                .input(input)
                .edge(TestGroups.EDGE)
                .build();

        // When / Then
        try {
            final Iterable<? extends Element> results = handler.doOperation(filter, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Edge group: " + TestGroups.EDGE + " does not exist in the schema"));
        }
    }

    @Test
    public void shouldFailValidationWhenElementFilterOperationIsNull() {
        // Given
        given(store.getSchema()).willReturn(SCHEMA);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .directed(true)
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        input.add(edge);
        input.add(edge1);

        final Filter filter = new Filter.Builder()
                .input(input)
                .edge(TestGroups.EDGE, new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(null)
                        .build())
                .build();

        try {
            final Iterable<? extends Element> results = handler.doOperation(filter, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains(filter.getClass().getSimpleName() + " contains a null function."));
        }
    }

    @Test
    public void shouldFailValidationWhenTypeArgumentOfPredicateIsIncorrect() {
        // Given
        final Schema schema  = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("junctionA")
                        .destination("junctionB")
                        .property(TestPropertyNames.COUNT, "count.long")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("junctionA")
                        .destination("junctionB")
                        .property(TestPropertyNames.COUNT, "count.long")
                        .build())
                .type("count.long", new TypeDefinition(Long.class))
                .build();

        given(store.getSchema()).willReturn(schema);

        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .property(TestPropertyNames.COUNT, 2L)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("junctionA")
                .dest("junctionB")
                .property(TestPropertyNames.COUNT, 1L)
                .build();

        input.add(edge);
        input.add(edge1);

        final Filter filter = new Filter.Builder()
                .input(input)
                .globalEdges(new ElementFilter.Builder()
                        .select(TestPropertyNames.COUNT)
                        .execute(new IsMoreThan(0D))
                        .build())
                .build();

        try {
            final Iterable<? extends Element> results = handler.doOperation(filter, context, store);
            fail("Exception expected");
        } catch (final OperationException e){
            assertTrue(e.getMessage().contains("is not compatible with the input type:"));
        }
    }
}
