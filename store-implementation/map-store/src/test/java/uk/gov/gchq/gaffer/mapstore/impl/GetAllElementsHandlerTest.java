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
package uk.gov.gchq.gaffer.mapstore.impl;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class GetAllElementsHandlerTest {
    static final String BASIC_ENTITY = "BasicEntity";
    static final String BASIC_EDGE1 = "BasicEdge";
    static final String BASIC_EDGE2 = "BasicEdge2";
    static final String PROPERTY1 = "property1";
    static final String PROPERTY2 = "property2";
    static final String COUNT = "count";
    private static final int NUM_LOOPS = 10;

    @Test
    public void testAddAndGetAllElementsNoAggregation() throws StoreException, OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        assertEquals(new HashSet<>(getElements()), resultsSet);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        assertEquals(new HashSet<>(getElements()), resultsSet);
    }

    @Test
    public void testAddAndGetAllElementsWithAggregation() throws StoreException, OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElementsForAggregation())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements getAllElements = new GetAllElements.Builder().build();
        final CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        final Entity entity = new Entity(BASIC_ENTITY, "0");
        entity.putProperty(PROPERTY1, "p");
        entity.putProperty(COUNT, NUM_LOOPS);
        expectedResults.add(entity);
        final Edge edge1 = new Edge.Builder()
                .group(BASIC_EDGE1)
                .source("A")
                .dest("B")
                .directed(true)
                .build();
        edge1.putProperty(PROPERTY1, "q");
        edge1.putProperty(COUNT, 2 * NUM_LOOPS);
        expectedResults.add(edge1);
        final Edge edge2 = new Edge.Builder()
                .group(BASIC_EDGE2)
                .source("X")
                .dest("Y")
                .directed(false)
                .build();
        edge2.putProperty(PROPERTY1, "r");
        edge2.putProperty(PROPERTY2, "s");
        edge2.putProperty(COUNT, 3 * (NUM_LOOPS / 2));
        expectedResults.add(edge2);
        final Edge edge3 = new Edge.Builder()
                .group(BASIC_EDGE2)
                .source("X")
                .dest("Y")
                .directed(false)
                .build();
        edge3.putProperty(PROPERTY1, "r");
        edge3.putProperty(PROPERTY2, "t");
        edge3.putProperty(COUNT, 3 * (NUM_LOOPS / 2));
        expectedResults.add(edge3);
        assertEquals(expectedResults, resultsSet);
    }

    static Map<Element, Integer> streamToCount(final Stream<? extends Element> elements) {
        final Map<Element, Integer> elementToCount = new HashMap<>();
        elements.forEach(element -> {
            if (elementToCount.containsKey(element)) {
                elementToCount.put(element, elementToCount.get(element) + 1);
            } else {
                elementToCount.put(element, 1);
            }
        });
        return elementToCount;
    }

    @Test
    public void testGetAllElementsWithViewRestrictedByGroup() throws OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE1)
                        .build())
                .build();
        final CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(BASIC_EDGE1))
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsWithViewRestrictedByGroupAndAPreAggregationFilter() throws OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(BASIC_EDGE1) && ((int) e.getProperty(COUNT)) > 5)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsWithViewRestrictedByGroupAndAPostAggregationFilter() throws OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(BASIC_EDGE1) && ((int) e.getProperty(COUNT)) > 5)
                .forEach(expectedResults::add);

        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsWithAndWithEntities() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When no entities
        GetAllElements getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2)
                        .build())
                .build();
        CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e instanceof Edge)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When view has entities
        getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE)
                        .edge(TestGroups.EDGE_2)
                        .build())
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsDirectedTypeOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .input(getElements())
                .build();
        graph.execute(addElements, new User());

        // When directedType is ALL
        GetAllElements getAllElements = new GetAllElements.Builder()
                .directedType(DirectedType.EITHER)
                .build();
        CloseableIterable<? extends Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        Streams.toStream(results).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When view has no edges
        getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Entity)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When directedType is DIRECTED
        getAllElements = new GetAllElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Entity || ((Edge) element).isDirected())
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When directedType is UNDIRECTED
        getAllElements = new GetAllElements.Builder()
                .directedType(DirectedType.UNDIRECTED)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        Streams.toStream(results).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Entity || !((Edge) element).isDirected())
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    public static Graph getGraph() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .addSchema(getSchema())
                .storeProperties(storeProperties)
                .build();
    }

    static Graph getGraphNoAggregation() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .addSchema(getSchemaNoAggregation())
                .storeProperties(storeProperties)
                .build();
    }

    static Graph getGraphNoIndices() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        storeProperties.setCreateIndex(false);
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphWithNoIndices")
                        .build())
                .addSchema(getSchema())
                .storeProperties(storeProperties)
                .build();
    }

    public static Schema getSchema() {
        return Schema.fromJson(StreamUtil.schemas(GetAllElementsHandlerTest.class));
    }

    static Schema getSchemaNoAggregation() {
        return Schema.fromJson(StreamUtil.openStreams(GetAllElementsHandlerTest.class, "schema-no-aggregation"));
    }

    public static List<Element> getElements() {
        final List<Element> elements = new ArrayList<>();
        IntStream.range(0, NUM_LOOPS)
                .forEach(i -> {
                    elements.add(new Entity.Builder()
                            .group(BASIC_ENTITY)
                            .vertex("" + i)
                            .property(PROPERTY1, "p")
                            .property(COUNT, 1)
                            .build());

                    elements.add(new Edge.Builder()
                            .group(BASIC_EDGE1)
                            .source("A")
                            .dest("B" + i)
                            .directed(true)
                            .property(PROPERTY1, "q")
                            .property(COUNT, i)
                            .build());

                    elements.add(new Edge.Builder()
                            .group(BASIC_EDGE2)
                            .source("X")
                            .dest("Y" + i)
                            .directed(false)
                            .property(PROPERTY1, "r")
                            .property(PROPERTY2, "s")
                            .property(COUNT, 3)
                            .build());
                });
        return elements;
    }

    private static List<Element> getElementsForAggregation() {
        final List<Element> elements = new ArrayList<>();
        IntStream.range(0, NUM_LOOPS)
                .forEach(i -> {
                    elements.add(new Entity.Builder()
                            .group(BASIC_ENTITY)
                            .vertex("0")
                            .property(PROPERTY1, "p")
                            .property(COUNT, 1)
                            .build());

                    elements.add(new Edge.Builder()
                            .group(BASIC_EDGE1)
                            .source("A")
                            .dest("B")
                            .directed(true)
                            .property(PROPERTY1, "q")
                            .property(COUNT, 2)
                            .build());

                    String property2;
                    if (i % 2 == 0) {
                        property2 = "s";
                    } else {
                        property2 = "t";
                    }

                    elements.add(new Edge.Builder()
                            .group(BASIC_EDGE2)
                            .source("X")
                            .dest("Y")
                            .directed(false)
                            .property(PROPERTY1, "r")
                            .property(PROPERTY2, property2)
                            .property(COUNT, 3)
                            .build());
                });
        return elements;
    }

    static List<Element> getDuplicateElements() {
        final List<Element> elements = new ArrayList<>();
        IntStream.range(0, NUM_LOOPS)
                .forEach(i -> {
                    elements.add(new Entity.Builder()
                            .group(BASIC_ENTITY)
                            .vertex("0")
                            .property(PROPERTY1, "p")
                            .property(COUNT, 1)
                            .build());

                    elements.add(new Edge.Builder()
                            .group(BASIC_EDGE1)
                            .source("A")
                            .dest("B")
                            .directed(true)
                            .property(PROPERTY1, "q")
                            .property(COUNT, 2)
                            .build());

                    elements.add(new Edge.Builder()
                            .group(BASIC_EDGE2)
                            .source("X")
                            .dest("Y")
                            .directed(false)
                            .property(PROPERTY1, "r")
                            .property(PROPERTY2, "s")
                            .property(COUNT, 3)
                            .build());
                });
        return elements;
    }
}
