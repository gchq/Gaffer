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

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class AggregateHandlerTest {
    private final Store store = mock(Store.class);
    private final Context context = new Context();
    private final AggregateHandler handler = new AggregateHandler();

    private final List<Element> input = new ArrayList<>();
    private final Set<Element> expected = new HashSet<>();

    private final Map<String, AggregatePair> edges = new HashMap<>();
    private final Map<String, AggregatePair> entities = new HashMap<>();

    private final Edge edge = new Edge.Builder()
            .group(TestGroups.EDGE)
            .property("timestamp", 2L)
            .property("turns", 6)
            .property("count", 2L)
            .build();

    private final Edge edge1 = new Edge.Builder()
            .group(TestGroups.EDGE)
            .property("timestamp", 1L)
            .property("turns", 9)
            .property("count", 5L)
            .build();

    private final Edge edge2 = new Edge.Builder()
            .group(TestGroups.EDGE)
            .property("timestamp", 2L)
            .property("turns", 4)
            .property("count", 1L)
            .build();

    private final Edge edge3 = new Edge.Builder()
            .group(TestGroups.EDGE_2)
            .property("timestamp", 4L)
            .property("length", 23)
            .property("count", 2L)
            .build();

    private final Edge edge4 = new Edge.Builder()
            .group(TestGroups.EDGE_2)
            .property("timestamp", 4L)
            .property("length", 17)
            .property("count", 3L)
            .build();

    private final Entity entity = new Entity.Builder()
            .group(TestGroups.ENTITY)
            .property("timestamp", 3L)
            .property("count", 3)
            .build();

    private final Entity entity1 = new Entity.Builder()
            .group(TestGroups.ENTITY)
            .property("timestamp", 2L)
            .property("count", 4)
            .build();

    private final Entity entity2 = new Entity.Builder()
            .group(TestGroups.ENTITY)
            .property("timestamp", 3L)
            .property("count", 2)
            .build();

    private final Entity entity3 = new Entity.Builder()
            .group(TestGroups.ENTITY)
            .property("timestamp", 2L)
            .property("count", 2)
            .build();

    @Test
    public void shouldAggregateElementsWhenNoGroupByInSchema() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        input.add(entity);
        input.add(entity1);
        input.add(entity2);

        final AggregatePair pair = new AggregatePair(
                new String[]{"timestamp"},
                new ElementAggregator.Builder()
                        .select("count")
                        .execute(new Sum())
                        .build());

        entities.put(TestGroups.ENTITY, pair);

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("timestamp", 3L)
                .property("count", 5)
                .build();

        expected.add(expectedEntity);
        expected.add(entity1);

        final Aggregate aggregate = new Aggregate.Builder()
                .input(input)
                .entities(entities)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(aggregate, context, store);
        final Set<Element> resultsSet = Sets.newHashSet(results);

        // Then
        assertEquals(expected, resultsSet);
    }

    @Test
    public void shouldAggregateElementsWhenAggregatorNotProvidedInSchema() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .groupBy("timestamp")
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        input.add(entity);
        input.add(entity1);
        input.add(entity2);
        input.add(entity3);

        final AggregatePair pair = new AggregatePair(
                new ElementAggregator.Builder()
                        .select("count")
                        .execute(new Sum())
                        .build());

        entities.put(TestGroups.ENTITY, pair);

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("timestamp", 3L)
                .property("count", 5)
                .build();

        final Entity expectedEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("timestamp", 2L)
                .property("count", 6)
                .build();

        expected.add(expectedEntity);
        expected.add(expectedEntity1);

        final Aggregate aggregate = new Aggregate.Builder()
                .input(input)
                .entities(entities)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(aggregate, context, store);
        final Set<Element> resultsSet = Sets.newHashSet(results);

        // Then
        assertEquals(expected, resultsSet);
    }

    @Test
    public void shouldAggregateTheSameFromSchemaOrOperation() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .build())
                .build();

        final Store store1 = mock(Store.class);
        final List<Element> input1 = new ArrayList<>();
        final Set<Element> expected1 = new HashSet<>();
        final Map<String, AggregatePair> edges1 = new HashMap<>();

        final Schema schema1 = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("count")
                                .execute(new Sum())
                                .select("turns")
                                .execute(new Sum())
                                .build())
                        .groupBy("timestamp")
                        .build())
                .build();

        given(store.getSchema()).willReturn(schema);
        given(store1.getSchema()).willReturn(schema1);

        input.add(edge);
        input.add(edge1);
        input.add(edge2);

        final Edge localEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 2L)
                .property("turns", 6)
                .property("count", 2L)
                .build();

        final Edge localEdge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 1L)
                .property("turns", 9)
                .property("count", 5L)
                .build();

        final Edge localEdge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 2L)
                .property("turns", 4)
                .property("count", 1L)
                .build();

        input1.add(localEdge);
        input1.add(localEdge1);
        input1.add(localEdge2);

        final AggregatePair pair = new AggregatePair(
                new String[]{"timestamp"},
                new ElementAggregator.Builder()
                        .select("count")
                        .execute(new Sum())
                        .select("turns")
                        .execute(new Sum())
                        .build());

        final AggregatePair otherPair = new AggregatePair();

        edges.put(TestGroups.EDGE, pair);
        edges1.put(TestGroups.EDGE, otherPair);

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 2L)
                .property("turns", 10)
                .property("count", 3L)
                .build();

        expected.add(expectedEdge);
        expected.add(edge1);

        expected1.add(expectedEdge);
        expected1.add(edge1);

        final Aggregate aggregate = new Aggregate.Builder()
                .input(input)
                .edges(edges)
                .build();

        final Aggregate aggregate1 = new Aggregate.Builder()
                .input(input1)
                .edges(edges1)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(aggregate, context, store);
        final Set<Element> resultsSet = Sets.newHashSet(results);

        final Iterable<? extends Element> results1 = handler.doOperation(aggregate1, context, store1);
        final Set<Element> resultsSet1 = Sets.newHashSet(results1);

        // Then
        assertEquals(expected, resultsSet);
        assertEquals(expected1, resultsSet1);
    }


    @Test
    public void shouldAggregateAMixOfEdgesAndEntities() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("turns")
                                .execute(new Max())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("count")
                                .execute(new Sum())
                                .build())
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(entity);
        input.add(entity1);
        input.add(entity2);
        input.add(entity3);

        final AggregatePair edgePair = new AggregatePair(
                new String[]{"timestamp"}
        );

        final AggregatePair entityPair = new AggregatePair(
                new String[]{"timestamp"}
        );

        edges.put(TestGroups.EDGE, edgePair);
        entities.put(TestGroups.ENTITY, entityPair);

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 2L)
                .property("turns", 6)
                .property("count", 2L)
                .build();

        final Edge expectedEdge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 1L)
                .property("turns", 9)
                .property("count", 5L)
                .build();

        final Entity expectedEntity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("timestamp", 2L)
                .property("count", 6)
                .build();

        final Entity expectedEntity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("timestamp", 3L)
                .property("count", 5)
                .build();

        expected.add(expectedEdge);
        expected.add(expectedEdge1);
        expected.add(expectedEntity);
        expected.add(expectedEntity1);

        final Aggregate aggregate = new Aggregate.Builder()
                .input(input)
                .edges(edges)
                .entities(entities)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(aggregate, context, store);
        final Set<Element> resultsSet = Sets.newHashSet(results);

        // Then
        assertEquals(expected, resultsSet);
    }

    @Test
    public void shouldAggregateEdgesFromMultipleGroups() throws OperationException {
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("turns")
                                .execute(new Max())
                                .build())
                        .groupBy("timestamp")
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("count")
                                .execute(new Sum())
                                .select("length")
                                .execute(new Sum())
                                .build())
                        .groupBy("timestamp")
                        .build())
                .build();

        given(store.getSchema()).willReturn(schema);

        input.add(edge);
        input.add(edge1);
        input.add(edge2);
        input.add(edge3);
        input.add(edge4);

        final AggregatePair pair = new AggregatePair();
        final AggregatePair pair1 = new AggregatePair();

        edges.put(TestGroups.EDGE, pair);
        edges.put(TestGroups.EDGE_2, pair1);

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 2L)
                .property("turns", 6)
                .property("count", 2L)
                .build();

        final Edge expectedEdge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 1L)
                .property("turns", 9)
                .property("count", 5L)
                .build();

        final Edge expectedEdge2 = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .property("timestamp", 4L)
                .property("length", 40)
                .property("count", 5L)
                .build();

        expected.add(expectedEdge);
        expected.add(expectedEdge1);
        expected.add(expectedEdge2);

        final Aggregate aggregate = new Aggregate.Builder()
                .input(input)
                .edges(edges)
                .build();

        // When
        final Iterable<? extends Element> results = handler.doOperation(aggregate, context, store);
        final Set<Element> resultsSet = Sets.newHashSet(results);

        // Then
        assertEquals(expected, resultsSet);
    }
}
