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
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.function.Aggregate;
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

    private final Map<String, Pair<String[], ElementAggregator>> edges = new HashMap<>();
    private final Map<String, Pair<String[], ElementAggregator>> entities = new HashMap<>();

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
            .property("timestamp", 1L)
            .property("turns", 4)
            .property("count", 1L)
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
    public void shouldAggregateElementsWithTransientProperty() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("timestamp")
                                .execute(new Max())
                                .build())
                        .groupBy("timestamp")
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        input.add(edge);
        input.add(edge1);
        input.add(edge2);

        final Pair<String[], ElementAggregator> pair = new Pair<>(
                new String[]{},
                new ElementAggregator.Builder()
                        .select("turns")
                        .execute(new Sum())
                        .select("count")
                        .execute(new Sum())
                        .build());

        edges.put(TestGroups.EDGE, pair);

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .property("timestamp", 2L)
                .property("turns", 19)
                .property("count", 8L)
                .build();

        expected.add(expectedEdge);

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

    @Test
    public void shouldAggregateElementsWhenNoGroupByInSchema() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .aggregator(new ElementAggregator.Builder()
                                .select("timestamp")
                                .execute(new Max())
                                .build())
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        input.add(entity);
        input.add(entity1);
        input.add(entity2);

        final Pair<String[], ElementAggregator> pair = new Pair<>(
                new String[] {"timestamp"},
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
    public void shouldAggregateElementsWhenAggregatorProvidedNotInSchema() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);

        input.add(entity);
        input.add(entity1);
        input.add(entity2);
        input.add(entity3);

        final Pair<String[], ElementAggregator> pair = new Pair<>(
                new String[] {"timestamp"},
                new ElementAggregator.Builder()
                        .select("timestamp")
                        .execute(new Max())
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
    public void shouldAggregateAMixOfEdgesAndEntities() {
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
                                .select("timestamp")
                                .execute(new Max())
                                .build())
                        .build())
                .build();
        given(store.getSchema()).willReturn(schema);
    }

    // todo none in schema <?>
    // entities and edges
    // multiple groups
}
