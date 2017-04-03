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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;

public class GetAllElementsHandlerTest {
    final static String BASIC_ENTITY = "BasicEntity";
    final static String BASIC_EDGE1 = "BasicEdge";
    final static String BASIC_EDGE2 = "BasicEdge2";
    final static String PROPERTY1 = "property1";
    final static String PROPERTY2 = "property2";
    final static String COUNT = "count";
    private final static int NUM_LOOPS = 10;

    @Test
    public void testAddAndGetAllElementsNoAggregation() throws StoreException, OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>().build();
        final CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(new HashSet<>(getElements()), resultsSet);

        // Repeat to ensure iterator can be consumed twice
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(new HashSet<>(getElements()), resultsSet);
    }

    @Test
    public void testAddAndGetAllElementsWithAggregation() throws StoreException, OperationException {
        // Given
        final Graph graph = getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElementsForAggregation())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>().build();
        final CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        final Entity entity = new Entity(BASIC_ENTITY, "0");
        entity.putProperty(PROPERTY1, "p");
        entity.putProperty(COUNT, NUM_LOOPS);
        expectedResults.add(entity);
        final Edge edge1 = new Edge(BASIC_EDGE1, "A", "B", true);
        edge1.putProperty(PROPERTY1, "q");
        edge1.putProperty(COUNT, 2 * NUM_LOOPS);
        expectedResults.add(edge1);
        final Edge edge2 = new Edge(BASIC_EDGE2, "X", "Y", false);
        edge2.putProperty(PROPERTY1, "r");
        edge2.putProperty(PROPERTY2, "s");
        edge2.putProperty(COUNT, 3 * (NUM_LOOPS / 2));
        expectedResults.add(edge2);
        final Edge edge3 = new Edge(BASIC_EDGE2, "X", "Y", false);
        edge3.putProperty(PROPERTY1, "r");
        edge3.putProperty(PROPERTY2, "t");
        edge3.putProperty(COUNT, 3 * (NUM_LOOPS / 2));
        expectedResults.add(edge3);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testAddAndGetAllElementsNoAggregationAndDuplicateElements() throws StoreException, OperationException {
        // Given
        final Graph graph = getGraphNoAggregation();
        final AddElements addElements = new AddElements.Builder()
                .elements(getDuplicateElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>().build();
        final CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Map<Element, Integer> resultingElementsToCount = streamToCount(
                StreamSupport.stream(results.spliterator(), false));
        final Map<Element, Integer> expectedCounts = streamToCount(getDuplicateElements().stream());
        assertEquals(expectedCounts, resultingElementsToCount);
    }

    static Map<Element, Integer> streamToCount(final Stream<Element> elements) {
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
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .view(new View.Builder()
                        .edge(BASIC_EDGE1)
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .view(new View.Builder()
                        .edge(BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
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
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .view(new View.Builder()
                        .edge(BASIC_EDGE1, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(COUNT)
                                        .execute(new IsMoreThan(5))
                                        .build())
                                .build())
                        .build())
                .build();
        final CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e.getGroup().equals(BASIC_EDGE1) && ((int) e.getProperty(COUNT)) > 5)
                .forEach(expectedResults::add);

        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsIncludeEntitiesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When includeEntities is false
        GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .includeEntities(false)
                .build();
        CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().stream()
                .filter(e -> e instanceof Edge)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When includeEntities is true
        getAllElements = new GetAllElements.Builder<>()
                .includeEntities(true)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsIncludeEdgesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When includeEdges is ALL
        GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .includeEdges(GetOperation.IncludeEdgeType.ALL)
                .build();
        CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When includeEdges is NONE
        getAllElements = new GetAllElements.Builder<>()
                .includeEdges(GetOperation.IncludeEdgeType.NONE)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Entity)
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When includeEdges is DIRECTED
        getAllElements = new GetAllElements.Builder<>()
                .includeEdges(GetOperation.IncludeEdgeType.DIRECTED)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Entity || ((Edge) element).isDirected())
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When includeEdges is UNDIRECTED
        getAllElements = new GetAllElements.Builder<>()
                .includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .filter(element -> element instanceof Entity || !((Edge) element).isDirected())
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsPopulatePropertiesOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When populateProperties is true
        GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .populateProperties(true)
                .build();
        CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        final Set<Element> expectedResults = new HashSet<>();
        getElements().forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);

        // When populateProperties is false
        getAllElements = new GetAllElements.Builder<>()
                .populateProperties(false)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        expectedResults.clear();
        getElements().stream()
                .map(element -> element.emptyClone())
                .forEach(expectedResults::add);
        assertEquals(expectedResults, resultsSet);
    }

    @Test
    public void testGetAllElementsLimitResultsOption() throws OperationException {
        // Given
        final Graph graph = GetAllElementsHandlerTest.getGraph();
        final AddElements addElements = new AddElements.Builder()
                .elements(getElements())
                .build();
        graph.execute(addElements, new User());

        // When limitResults is 1
        GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .limitResults(1)
                .build();
        CloseableIterable<Element> results = graph.execute(getAllElements, new User());

        // Then
        final Set<Element> resultsSet = new HashSet<>();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(1, resultsSet.size());

        // When limitResults is 2
        getAllElements = new GetAllElements.Builder<>()
                .limitResults(2)
                .build();
        results = graph.execute(getAllElements, new User());

        // Then
        resultsSet.clear();
        StreamSupport.stream(results.spliterator(), false).forEach(resultsSet::add);
        assertEquals(2, resultsSet.size());
    }

    public static Graph getGraph() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        return new Graph.Builder()
                .addSchema(getSchema())
                .storeProperties(storeProperties)
                .build();
    }

    static Graph getGraphNoAggregation() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        return new Graph.Builder()
                .addSchema(getSchemaNoAggregation())
                .storeProperties(storeProperties)
                .build();
    }

    static Graph getGraphNoIndices() {
        final MapStoreProperties storeProperties = new MapStoreProperties();
        storeProperties.setCreateIndex("false");
        return new Graph.Builder()
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
                    final Entity entity = new Entity(BASIC_ENTITY, "" + i);
                    entity.putProperty(PROPERTY1, "p");
                    entity.putProperty(COUNT, 1);
                    elements.add(entity);
                    final Edge edge1 = new Edge(BASIC_EDGE1, "A", "B" + i, true);
                    edge1.putProperty(PROPERTY1, "q");
                    edge1.putProperty(COUNT, i);
                    elements.add(edge1);
                    final Edge edge2 = new Edge(BASIC_EDGE2, "X", "Y" + i, false);
                    edge2.putProperty(PROPERTY1, "r");
                    edge2.putProperty(PROPERTY2, "s");
                    edge2.putProperty(COUNT, 3);
                    elements.add(edge2);
                });
        return elements;
    }

    private static List<Element> getElementsForAggregation() {
        final List<Element> elements = new ArrayList<>();
        IntStream.range(0, NUM_LOOPS)
                .forEach(i -> {
                    final Entity entity = new Entity(BASIC_ENTITY, "0");
                    entity.putProperty(PROPERTY1, "p");
                    entity.putProperty(COUNT, 1);
                    elements.add(entity);
                    final Edge edge1 = new Edge(BASIC_EDGE1, "A", "B", true);
                    edge1.putProperty(PROPERTY1, "q");
                    edge1.putProperty(COUNT, 2);
                    elements.add(edge1);
                    final Edge edge2 = new Edge(BASIC_EDGE2, "X", "Y", false);
                    edge2.putProperty(PROPERTY1, "r");
                    String property2;
                    if (i % 2 == 0) {
                        property2 = "s";
                    } else {
                        property2 = "t";
                    }
                    edge2.putProperty(PROPERTY2, property2);
                    edge2.putProperty(COUNT, 3);
                    elements.add(edge2);
                });
        return elements;
    }

    static List<Element> getDuplicateElements() {
        final List<Element> elements = new ArrayList<>();
        IntStream.range(0, NUM_LOOPS)
                .forEach(i -> {
                    final Entity entity = new Entity(BASIC_ENTITY, "0");
                    entity.putProperty(PROPERTY1, "p");
                    entity.putProperty(COUNT, 1);
                    elements.add(entity);
                    final Edge edge1 = new Edge(BASIC_EDGE1, "A", "B", true);
                    edge1.putProperty(PROPERTY1, "q");
                    edge1.putProperty(COUNT, 2);
                    elements.add(edge1);
                    final Edge edge2 = new Edge(BASIC_EDGE2, "X", "Y", false);
                    edge2.putProperty(PROPERTY1, "r");
                    edge2.putProperty(PROPERTY2, "s");
                    edge2.putProperty(COUNT, 3);
                    elements.add(edge2);
                });
        return elements;
    }
}
