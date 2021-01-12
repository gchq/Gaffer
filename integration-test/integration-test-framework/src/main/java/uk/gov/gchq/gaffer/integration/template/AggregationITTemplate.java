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

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Product;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEdges;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEntities;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.jsonClone;

public class AggregationITTemplate extends AbstractStoreIT {
    private static final String AGGREGATED_SOURCE = SOURCE + 6;
    private static final String AGGREGATED_DEST = DEST + 6;

    private static final int NON_AGGREGATED_ID = 8;
    private static final String NON_AGGREGATED_SOURCE = SOURCE + NON_AGGREGATED_ID;
    private static final String NON_AGGREGATED_DEST = DEST + NON_AGGREGATED_ID;

    @GafferTest
    @TraitRequirement(StoreTrait.INGEST_AGGREGATION)
    public void shouldAggregateIdenticalElements(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        addDuplicateElements(graph);
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(AGGREGATED_SOURCE))
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, new User()));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());

        final Entity expectedEntity = new Entity(TestGroups.ENTITY, AGGREGATED_SOURCE);
        expectedEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
        expectedEntity.putProperty(TestPropertyNames.COUNT, 3L);

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(AGGREGATED_SOURCE)
                .dest(AGGREGATED_DEST)
                .directed(false)
                .build();
        expectedEdge.putProperty(TestPropertyNames.INT, 1);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 2L);

        ElementUtil.assertElementEquals(Arrays.asList(expectedEdge, expectedEntity), results);
    }

    @GafferTest
    @TraitRequirement(StoreTrait.INGEST_AGGREGATION)
    public void shouldAggregateElementsWithNoGroupBy(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        addDuplicateElements(graph);
        final String vertex = "testVertex1";
        final long timestamp = System.currentTimeMillis();

        graph.execute(new AddElements.Builder()
                .input(new Entity.Builder()
                                .group(TestGroups.ENTITY_2)
                                .vertex(vertex)
                                .property(TestPropertyNames.INT, 1)
                                .property(TestPropertyNames.TIMESTAMP, timestamp)
                                .build(),
                        new Entity.Builder()
                                .group(TestGroups.ENTITY_2)
                                .vertex(vertex)
                                .property(TestPropertyNames.INT, 2)
                                .property(TestPropertyNames.TIMESTAMP, timestamp)
                                .build())
                .build(), new User());

        graph.execute(new AddElements.Builder()
                .input(new Entity.Builder()
                                .group(TestGroups.ENTITY_2)
                                .vertex(vertex)
                                .property(TestPropertyNames.INT, 2)
                                .property(TestPropertyNames.TIMESTAMP, timestamp)
                                .build(),
                        new Entity.Builder()
                                .group(TestGroups.ENTITY_2)
                                .vertex(vertex)
                                .property(TestPropertyNames.INT, 3)
                                .property(TestPropertyNames.TIMESTAMP, timestamp)
                                .build(),
                        new Entity.Builder()
                                .group(TestGroups.ENTITY_2)
                                .vertex(vertex)
                                .property(TestPropertyNames.INT, 9)
                                .property(TestPropertyNames.TIMESTAMP, timestamp)
                                .build())
                .build(), new User());

        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed(vertex))
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, new User()));

        // Then
        assertNotNull(results);
        assertEquals(1, results.size());

        final Entity expectedEntity2 = new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .vertex(vertex)
                .property(TestPropertyNames.INT, 9)
                .property(TestPropertyNames.TIMESTAMP, timestamp)
                .build();

        ElementUtil.assertElementEquals(
                Collections.singletonList(expectedEntity2),
                results
        );
    }

    @GafferTest
    @TraitRequirement(StoreTrait.INGEST_AGGREGATION)
    public void shouldNotAggregateEdgesWithDifferentDirectionFlag(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        addDuplicateElements(graph);
        final GetElements getEdges = new GetElements.Builder()
                .input(new EntitySeed(NON_AGGREGATED_SOURCE))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getEdges, new User()));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        ElementUtil.assertElementEquals(
                Arrays.asList(
                        getEdges().get(new EdgeSeed(NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, false)),
                        new Edge.Builder().group(TestGroups.EDGE)
                                .source(NON_AGGREGATED_SOURCE)
                                .dest(NON_AGGREGATED_DEST)
                                .directed(true)
                                .build()),
                results
        );
    }

    @GafferTest
    @TraitRequirement({StoreTrait.PRE_AGGREGATION_FILTERING, StoreTrait.QUERY_AGGREGATION})
    public void shouldGetAllElementsWithFilterSummarisation(final GafferTestCase testCase) throws Exception {
        Graph graph = testCase.getPopulatedGraph();
        addDuplicateElements(graph);

        final Edge edge1 = TestUtil.getEdges().get(new EdgeSeed(SOURCE_1, DEST_1, false)).emptyClone();
        edge1.putProperty(TestPropertyNames.INT, 100);
        edge1.putProperty(TestPropertyNames.COUNT, 1L);

        final Edge edge2 = edge1.emptyClone();
        edge2.putProperty(TestPropertyNames.INT, 101);
        edge2.putProperty(TestPropertyNames.COUNT, 1L);

        graph.execute(new AddElements.Builder()
                .input(edge1, edge2)
                .build(), new User());

        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.INT)
                                        .execute(new IsIn(Arrays.asList(100, 101)))
                                        .build())
                                .groupBy()
                                .aggregator(new ElementAggregator.Builder()
                                        .select(TestPropertyNames.INT)
                                        .execute(new Product())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, new User());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);

        assertEquals(1, resultList.size());
        // aggregation is has been replaced with Product
        assertEquals(10100, resultList.get(0).getProperty(TestPropertyNames.INT));
    }

    private void addDuplicateElements(final Graph graph) throws OperationException {
        // Add duplicate elements
        Entity aggregatedEntity = jsonClone(getEntities().get(new EntitySeed(AGGREGATED_SOURCE)));
        graph.execute(new AddElements.Builder()
                .input(aggregatedEntity, aggregatedEntity)
                .build(), new User());

        graph.execute(new AddElements.Builder()
                .input(getEdges().get(new EdgeSeed(AGGREGATED_SOURCE, AGGREGATED_DEST, false)))
                .build(), new User());

        // Edge with existing ids but directed
        graph.execute(new AddElements.Builder()
                .input(new Edge.Builder().group(TestGroups.EDGE)
                        .source(NON_AGGREGATED_SOURCE)
                        .dest(NON_AGGREGATED_DEST)
                        .directed(true)
                        .build())
                .build(), new User());
    }
}
