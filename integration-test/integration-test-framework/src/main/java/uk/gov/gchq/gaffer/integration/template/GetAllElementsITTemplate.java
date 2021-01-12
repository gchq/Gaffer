/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.function.ApplyBiFunction;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEdges;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEntities;

public class GetAllElementsITTemplate extends AbstractStoreIT {

    @GafferTest
    @TraitRequirement({StoreTrait.INGEST_AGGREGATION})
    public void shouldGetAllElements(final GafferTestCase testCase) throws Exception {
        Graph graph = testCase.getPopulatedGraph();
        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final boolean includeEdges : Arrays.asList(true, false)) {
                if (!includeEntities && !includeEdges) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final DirectedType directedType : DirectedType.values()) {
                    try {
                        shouldGetAllElements(includeEntities, includeEdges, directedType, graph);
                    } catch (final AssertionError e) {
                        throw new AssertionError("GetAllElements failed with parameters: includeEntities=" + includeEntities
                                + ", includeEdges=" + includeEdges + ", directedType=" + directedType.name(), e);
                    }
                }
            }
        }
    }

    @GafferTest
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void shouldGetAllElementsWithFilterWithoutSummarisation(final GafferTestCase testCase) throws Exception {
        Graph graph = testCase.getPopulatedGraph();
        final Edge edge1 = getEdges().get(new EdgeSeed(SOURCE_1, DEST_1, false)).emptyClone();
        edge1.putProperty(TestPropertyNames.INT, 100);
        edge1.putProperty(TestPropertyNames.COUNT, 1L);

        final Edge edge2 = edge1.emptyClone();
        edge2.putProperty(TestPropertyNames.INT, 101);
        edge2.putProperty(TestPropertyNames.COUNT, 1L);

        graph.execute(new AddElements.Builder()
                        .input(edge1, edge2)
                        .build(),
                new User());

        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.INT)
                                        .execute(new IsIn(Arrays.asList((Object) 100, 101)))
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, new User());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(2, resultList.size());
        assertTrue(resultList.containsAll(Lists.newArrayList(edge1, edge2)));
    }

    @GafferTest
    @TraitRequirement({StoreTrait.PRE_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    public void shouldGetAllElementsFilteredOnGroup(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, new User());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(getEntities().size(), resultList.size());
        for (final Element element : resultList) {
            assertEquals(TestGroups.ENTITY, element.getGroup());
        }
    }

    @GafferTest
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void shouldGetAllFilteredElements(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = testCase.getPopulatedGraph();
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

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, new User());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertEquals("A1", ((Entity) resultList.get(0)).getVertex());
    }

    @GafferTest
    @TraitRequirement({StoreTrait.TRANSFORMATION, StoreTrait.PRE_AGGREGATION_FILTERING})
    public void shouldGetAllTransformedFilteredElements(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.VERTEX.name())
                                        .execute(new IsEqual("A1"))
                                        .build())
                                .transientProperty(TestPropertyNames.TRANSIENT_1, String.class)
                                .transformer(new ElementTransformer.Builder()
                                        .select(IdentifierType.VERTEX.name(),
                                                TestPropertyNames.SET)
                                        .execute(new Concat())
                                        .project(TestPropertyNames.TRANSIENT_1)
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, new User());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertEquals(
                "A1,[3]",
                resultList.get(0).getProperties().get(TestPropertyNames.TRANSIENT_1)
        );
    }

    @GafferTest
    public void shouldGetAllElementsWithProvidedProperties(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final User user = new User();

        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        for (final Element result : results) {
            assertEquals(1, result.getProperties().size());
            assertEquals(1L, result.getProperties().get(TestPropertyNames.COUNT));
        }
    }

    @GafferTest
    public void shouldGetAllElementsWithExcludedProperties(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final User user = new User();

        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        for (final Element result : results) {
            assertEquals(1, result.getProperties().size());
            assertEquals(1L, result.getProperties().get(TestPropertyNames.COUNT));
        }
    }

    protected void shouldGetAllElements(final boolean includeEntities, final boolean includeEdges, final DirectedType directedType, final Graph graph) throws Exception {
        // Given
        final List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(getEntities().values());
        }

        if (includeEdges) {
            for (final Edge edge : getEdges().values()) {
                if (DirectedType.EITHER == directedType
                        || (edge.isDirected() && DirectedType.DIRECTED == directedType)
                        || (!edge.isDirected() && DirectedType.UNDIRECTED == directedType)) {
                    expectedElements.add(edge);
                }
            }
        }

        final View.Builder viewBuilder = new View.Builder();
        if (includeEntities) {
            viewBuilder.entity(TestGroups.ENTITY);
        }
        if (includeEdges) {
            viewBuilder.edge(TestGroups.EDGE);
        }
        final GetAllElements op = new GetAllElements.Builder()
                .directedType(directedType)
                .view(viewBuilder.build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, new User());

        // Then
        final List<Element> expectedElementsCopy = Lists.newArrayList(expectedElements);
        for (final Element result : results) {
            final ElementId seed = ElementSeed.createSeed(result);
            if (result instanceof Entity) {
                Entity entity = (Entity) result;
                assertTrue(expectedElements.contains(entity), "Entity was not expected: " + entity);
            } else {
                Edge edge = (Edge) result;
                if (edge.isDirected()) {
                    assertTrue(expectedElements.contains(edge), "Edge was not expected: " + edge);
                } else {
                    final Edge edgeReversed = new Edge.Builder()
                            .group(TestGroups.EDGE)
                            .source(edge.getDestination())
                            .dest(edge.getSource())
                            .directed(edge.isDirected())
                            .build();
                    expectedElementsCopy.remove(edgeReversed);
                    assertTrue(expectedElements.contains(result) || expectedElements.contains(edgeReversed), "Edge was not expected: " + seed);
                }
            }
            expectedElementsCopy.remove(result);
        }

        assertEquals(expectedElements.size(),
                Lists.newArrayList(results).size(), "The number of elements returned was not as expected. Missing elements: " + expectedElementsCopy);
    }

    @GafferTest
    @TraitRequirement({StoreTrait.TRANSFORMATION})
    public void shouldAllowBiFunctionInView(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final Map<String, Class<?>> transientProperties = new HashMap<>();
        transientProperties.put("propLong", Long.class);
        transientProperties.put("combined", Long.class);

        final List<TupleAdaptedFunction<String, ?, ?>> transformFunctions = new ArrayList<>();

        final TupleAdaptedFunction<String, Integer, Long> convertToLong = new TupleAdaptedFunction<>();
        convertToLong.setSelection(new String[]{TestPropertyNames.INT});
        convertToLong.setFunction((Function) new ToLong());
        convertToLong.setProjection(new String[]{"propLong"});

        final TupleAdaptedFunction<String, Integer, Long> sum = new TupleAdaptedFunction<>();
        sum.setSelection(new String[]{"propLong", TestPropertyNames.COUNT});
        sum.setFunction(new ApplyBiFunction(new Sum()));
        sum.setProjection(new String[]{"combined"});

        transformFunctions.add(convertToLong);
        transformFunctions.add(sum);

        final GetAllElements get = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .transientProperties(transientProperties)
                                .addTransformFunctions(transformFunctions)
                                .build())
                        .build())
                .build();

        final CloseableIterable<? extends Element> results = graph.execute(get, new User());

        for (final Element result : results) {

            final Long expectedResult = (Long) result.getProperty("propLong") + (Long) result.getProperty(TestPropertyNames.COUNT);
            final Long combined = (Long) result.getProperty("combined");
            assertEquals(expectedResult, combined);
        }
    }
}
