/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template.loader;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.LoaderTestCase;
import uk.gov.gchq.gaffer.integration.template.loader.schemas.ISchemaLoader;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR_2;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR_3;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_2;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_3;

public abstract class AbstractLoaderIT {

    @TempDir
    public static Path tempDir;

    private static final User BASIC_USER = new User("basic", Sets.newHashSet("public"));
    private static final User PRIVILEGED_USER = new User("privileged", Sets.newHashSet("public", "private"));

    private <T> List<T> duplicate(final Iterable<T> items) {
        final List<T> duplicates = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Iterables.addAll(duplicates, items);
        }
        return duplicates;
    }

    protected void beforeEveryTest(final LoaderTestCase testCase) throws Exception {
        // Load the data
        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();
        addElements(graph, loader);
    }

    protected abstract void addElements(final Graph graph, final Iterable<? extends Element> input) throws OperationException;

    protected void addElements(final Graph graph, final ISchemaLoader loader) throws OperationException {
        // Duplicate the elements as per the original behaviour and add
        Iterable<Element> elements = Iterables.concat(duplicate(loader.createEdges().values()),
            duplicate(loader.createEntities().values()));
        addElements(graph, elements);
    }

    //////////////////////////////////////////////////////////////////
    //                         Get Elements                         //
    //////////////////////////////////////////////////////////////////

    @GafferTest
    protected void shouldGetAllElements(final LoaderTestCase testCase) throws Exception {
        beforeEveryTest(testCase);

        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();

        getAllElements(graph, PRIVILEGED_USER, loader);
    }

    @GafferTest
    protected void shouldGetAllElementsWithProvidedProperties(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();

        final View view = new View.Builder()
            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                .properties(TestPropertyNames.COUNT)
                .build())
            .build();

        // When
        final Consumer<Iterable<? extends Element>> resultTest = iter -> {
            iter.forEach(element -> {
                assertEquals(1, element.getProperties().size());
                assertEquals((long) 2, element.getProperties().get(TestPropertyNames.COUNT));
            });
        };

        // Then
        getAllElementsWithView(resultTest, graph, PRIVILEGED_USER, view);
    }

    @GafferTest
    protected void shouldGetAllElementsWithExcludedProperties(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();

        final View view = new View.Builder()
            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                .excludeProperties(TestPropertyNames.COUNT)
                .build())
            .build();

        final GetAllElements op = new GetAllElements.Builder()
            .view(view)
            .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, PRIVILEGED_USER);

        // When
        final List<Element> expected = getQuerySummarisedEdges(graph, loader, view).stream().map(edge -> {
            edge.getProperties().remove(TestPropertyNames.COUNT);
            return edge;
        }).collect(toList());

        //Then
        assertElementEquals(expected, results);
    }

    @GafferTest
    protected void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetElements(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();

        // When
        final GetElements op = new GetElements.Builder()
            .input(new EmptyClosableIterable<>())
            .build();

        // Then
        final CloseableIterable<? extends Element> results = graph.execute(op, PRIVILEGED_USER);

        assertFalse(results.iterator().hasNext());
    }

    @GafferTest
    protected void shouldGetElementsWithMatchedVertex(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();

        // Then
        final View view = new View.Builder()
            .edge(TestGroups.EDGE)
            .build();
        final GetElements op = new GetElements.Builder()
            .input(new EntitySeed(SOURCE_DIR_1), new EntitySeed(DEST_DIR_2), new EntitySeed(SOURCE_DIR_3))
            .view(view)
            .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, PRIVILEGED_USER);

        assertElementEquals(getQuerySummarisedEdges(graph, loader, view)
            .stream()
            .filter(Edge::isDirected)
            .filter(edge -> {
                final List<String> vertices = Lists.newArrayList(SOURCE_DIR_1, SOURCE_DIR_2, SOURCE_DIR_3);
                return vertices.contains(edge.getMatchedVertexValue());
            })
            .collect(toList()), results);
    }

    //////////////////////////////////////////////////////////////////
    //                         Visibility                           //
    //////////////////////////////////////////////////////////////////

    @GafferTest
    @TraitRequirement(StoreTrait.VISIBILITY)
    protected void shouldGetOnlyVisibleElements(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();

        // Then
        getAllElements(graph, BASIC_USER, loader);
    }

    //////////////////////////////////////////////////////////////////
    //                         Filtering                            //
    //////////////////////////////////////////////////////////////////
    @TraitRequirement({StoreTrait.PRE_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    @GafferTest
    public void shouldGetAllElementsFilteredOnGroup(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();

        // When
        final GetAllElements op = new GetAllElements.Builder()
            .view(new View.Builder()
                .entity(TestGroups.ENTITY)
                .build())
            .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, PRIVILEGED_USER);

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(loader.createEntities().size(), resultList.size());
        for (final Element element : resultList) {
            assertEquals(TestGroups.ENTITY, element.getGroup());
        }
    }

    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    @GafferTest
    public void shouldGetAllFilteredElements(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();

        // When
        final GetAllElements op = new GetAllElements.Builder()
            .view(new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                        .select(IdentifierType.VERTEX.name())
                        .execute(new IsEqual("A1"))
                        .build())
                    .build())
                .build())
            .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, PRIVILEGED_USER);

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertEquals("A1", ((Entity) resultList.get(0)).getVertex());
    }

    @TraitRequirement({StoreTrait.MATCHED_VERTEX, StoreTrait.QUERY_AGGREGATION})
    @GafferTest
    public void shouldGetElementsWithMatchedVertexFilter(final LoaderTestCase testCase) throws Exception {
        // Given
        beforeEveryTest(testCase);
        Graph graph = testCase.getGraph();
        ISchemaLoader loader = testCase.getSchemaSetup().getLoader();

        // When
        final View view = new View.Builder()
            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                    .select(IdentifierType.ADJACENT_MATCHED_VERTEX.name())
                    .execute(new IsIn(DEST_DIR_1, DEST_DIR_2, DEST_DIR_3))
                    .build())
                .build())
            .build();
        final GetElements op = new GetElements.Builder()
            .input(new EntitySeed(SOURCE_DIR_1), new EntitySeed(DEST_DIR_2), new EntitySeed(SOURCE_DIR_3))
            .view(view)
            .build();

        // Then
        final CloseableIterable<? extends Element> results = graph.execute(op, PRIVILEGED_USER);

        // Then
        assertElementEquals(getQuerySummarisedEdges(graph, loader, view)
            .stream()
            .filter(Edge::isDirected)
            .filter(edge -> {
                final List<String> vertices = Lists.newArrayList(SOURCE_DIR_1, DEST_DIR_2, SOURCE_DIR_3);
                return vertices.contains(edge.getMatchedVertexValue());
            })
            .filter(edge -> {
                final List<String> vertices = Lists.newArrayList(DEST_DIR_1, DEST_DIR_2, DEST_DIR_3);
                return vertices.contains(edge.getAdjacentMatchedVertexValue());
            })
            .collect(toList()), results);
    }

    private void getAllElements(final Graph graph, final User user, final ISchemaLoader loader) throws Exception {
        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final boolean includeEdges : Arrays.asList(true, false)) {
                if (!includeEntities && !includeEdges) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final DirectedType directedType : DirectedType.values()) {
                    try {
                        final View.Builder viewBuilder = new View.Builder();
                        if (includeEntities) {
                            viewBuilder.entity(TestGroups.ENTITY);
                        }
                        if (includeEdges) {
                            viewBuilder.edge(TestGroups.EDGE);
                        }
                        getAllElements(graph, user, loader, includeEntities, includeEdges, directedType, viewBuilder.build());
                    } catch (final AssertionError e) {
                        throw new AssertionError("GetAllElements failed with extensions: includeEntities=" + includeEntities
                            + ", includeEdges=" + includeEdges + ", directedType=" + directedType.name(), e);
                    }
                }
            }
        }
    }


    private void getAllElementsWithView(final Consumer<Iterable<? extends Element>> resultTester, final Graph graph, final User user, final View view) throws Exception {
        for (final DirectedType directedType : DirectedType.values()) {
            try {
                getAllElements(resultTester, graph, user, directedType, view);
            } catch (final AssertionError e) {
                throw new AssertionError("GetAllElements failed with extensions: includeEntities=" + view.hasEntities()
                    + ", includeEdges=" + view.hasEdges() + ", directedType=" + directedType.name(), e);
            }
        }
    }

    private void getAllElements(final Consumer<Iterable<? extends Element>> resultTester, final Graph graph, final User user, final DirectedType directedType, final View view) throws Exception {
        // Given
        final GetAllElements op = new GetAllElements.Builder()
            .directedType(directedType)
            .view(view)
            .build();

        // When
        final Iterable<? extends Element> results = graph.execute(op, user);

        // Then
        resultTester.accept(results);
    }

    private List<Entity> getIngestSummarisedEntities(final Graph graph, final ISchemaLoader schemaLoader) {
        final Schema schema = graph.getSchema();
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.ingestAggregate(duplicate(schemaLoader.createEntities().values()), schema));
    }
//
//    private List<Entity> getQuerySummarisedEntities() {
//        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
//        final View view = new View.Builder()
//            .entities(schema.getEntityGroups())
//            .edges(schema.getEdgeGroups())
//            .build();
//        return getQuerySummarisedEntities(view);
//    }
//
    private List<Entity> getQuerySummarisedEntities(final Graph graph, final ISchemaLoader schemaLoader, final View view) {
        final Schema schema = graph.getSchema();
        final List<Entity> ingestSummarisedEntities = getIngestSummarisedEntities(graph, schemaLoader);
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.queryAggregate(ingestSummarisedEntities, schema, view));
    }

    private List<Edge> getIngestSummarisedEdges(final Graph graph, final ISchemaLoader loader) {
        final Schema schema = graph.getSchema();
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.ingestAggregate(duplicate(loader.createEdges().values()), schema));
    }
//
//    private List<Edge> getQuerySummarisedEdges() {
//        final Schema schema = null != graph ? graph.getSchema() : getStoreSchema();
//        final View view = new View.Builder()
//            .entities(schema.getEntityGroups())
//            .edges(schema.getEdgeGroups())
//            .build();
//        return getQuerySummarisedEdges(view);
//    }

    private List<Edge> getQuerySummarisedEdges(final Graph graph, final ISchemaLoader loader, final View view) {
        final Schema schema = graph.getSchema();
        final List<Edge> ingestSummarisedEdges = getIngestSummarisedEdges(graph, loader);
        return (List) Lists.newArrayList((Iterable) AggregatorUtil.queryAggregate(ingestSummarisedEdges, schema, view));
    }

    private void getAllElements(final Graph graph, final User user, final ISchemaLoader loader, final boolean includeEntities, final boolean includeEdges, final DirectedType directedType, final View view) throws Exception {
        // Given
        List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(getQuerySummarisedEntities(graph, loader, view));
        }

        if (includeEdges) {
            for (final Edge edge : getQuerySummarisedEdges(graph, loader, view)) {
                if (DirectedType.EITHER == directedType
                    || (edge.isDirected() && DirectedType.DIRECTED == directedType)
                    || (!edge.isDirected() && DirectedType.UNDIRECTED == directedType)) {
                    expectedElements.add(edge);
                }
            }
        }

        if (!user.getDataAuths().isEmpty()) {
            final String dataAuths = user.getDataAuths().stream().collect(Collectors.joining(","));
            final List<Element> nonVisibleElements = expectedElements.stream()
                .filter(e -> {
                    final String visibility = (String) e.getProperties().get(TestPropertyNames.VISIBILITY);
                    if (null != visibility) {
                        return !dataAuths.contains(visibility);
                    } else {
                        return false;
                    }
                }).collect(toList());

            expectedElements.removeAll(nonVisibleElements);
        }

        getAllElements(graph, user, expectedElements, directedType, view);
    }

    private void getAllElements(final Graph graph, final User user, final List<Element> expectedElements, final DirectedType directedType, final View view) throws Exception {
        // Given
        final GetAllElements op = new GetAllElements.Builder()
            .directedType(directedType)
            .view(view)
            .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        assertElementEquals(expectedElements, results);
    }
}
