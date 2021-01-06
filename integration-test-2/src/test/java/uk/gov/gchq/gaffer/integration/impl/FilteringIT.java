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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEdges;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEntities;

public class FilteringIT extends AbstractStoreIT {

    @GafferTest
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringGroups(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final List<ElementId> seeds = Collections.singletonList(new EntitySeed("A3"));

        final GetElements getElementsWithoutFiltering =
                new GetElements.Builder()
                        .input(seeds)
                        .build();

        final GetElements getElementsWithFiltering = new GetElements.Builder()
                .input(seeds)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When - without filtering
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, new User()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, new User()));

        // Then - without filtering
        assertNotNull(resultsWithoutFiltering);
        assertEquals(9, resultsWithoutFiltering.size());
        ElementUtil.assertElementEquals(resultsWithoutFiltering, Arrays.asList(
            getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),

                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),

                getEntities().get(new EntitySeed("A3"))));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(1, resultsWithFiltering.size());
        ElementUtil.assertElementEquals(resultsWithFiltering, Arrays.asList(
                (Element) getEntities().get(new EntitySeed("A3"))
        ));
    }

    @GafferTest
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringIdentifiers(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final List<ElementId> seeds = Collections.singletonList(new EntitySeed("A3"));

        final GetElements getElementsWithoutFiltering =
                new GetElements.Builder()
                        .input(seeds)
                        .build();

        final GetElements getElementsWithFiltering = new GetElements.Builder()
                .input(seeds)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.DESTINATION.name())
                                        .execute(new IsEqual("B3"))
                                        .build())
                                .build())
                        .build())
                .build();

        // When - without filtering
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, new User()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, new User()));

        // Then - without filtering
        assertNotNull(resultsWithoutFiltering);
        assertEquals(9, resultsWithoutFiltering.size());
        ElementUtil.assertElementEquals(resultsWithoutFiltering, Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),

                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),

                getEntities().get(new EntitySeed("A3"))));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(3, resultsWithFiltering.size());
        ElementUtil.assertElementEquals(resultsWithFiltering, Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEntities().get(new EntitySeed("A3"))));
    }

    @GafferTest
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringProperties(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final List<ElementId> seeds = Arrays.asList(new EntitySeed("A3"),
                new EdgeSeed("A5", "B5", false));

        final GetElements getElementsWithoutFiltering =
                new GetElements.Builder()
                        .input(seeds)
                        .build();

        final GetElements getElementsWithFiltering = new GetElements.Builder()
                .input(seeds)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.VERTEX.name())
                                        .execute(new IsEqual("A5"))
                                        .build())
                                .build())
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.INT)
                                        .execute(new IsLessThan(2))
                                        .build())
                                .build())
                        .build())
                .build();

        // When - without filtering
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(
                graph.execute(getElementsWithoutFiltering, new User()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(
                graph.execute(getElementsWithFiltering, new User()));

        // Then - without filtering
        List<Element> expectedResults = Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),
                getEdges().get(new EdgeSeed("A5", "B5", false)),

                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),

                getEntities().get(new EntitySeed("A3")),
                getEntities().get(new EntitySeed("A5")),
                getEntities().get(new EntitySeed("B5"))
        );
        ElementUtil.assertElementEquals(expectedResults, resultsWithoutFiltering);

        // Then - with filtering
        List<Element> expectedFilteredResults = Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),
                getEdges().get(new EdgeSeed("A5", "B5", false)),
                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),
                getEntities().get(new EntitySeed("A5"))
        );
        ElementUtil.assertElementEquals(expectedFilteredResults, resultsWithFiltering);
    }

    @GafferTest
    @TraitRequirement({StoreTrait.POST_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    public void testPostAggregationFilteringIdentifiers(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final List<ElementId> seeds = Collections.singletonList(new EntitySeed("A3"));

        final GetElements getElementsWithoutFiltering =
                new GetElements.Builder()
                        .input(seeds)
                        .build();

        final GetElements getElementsWithFiltering = new GetElements.Builder()
                .input(seeds)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.DESTINATION.name())
                                        .execute(new IsEqual("B3"))
                                        .build())
                                .build())
                        .build())
                .build();

        // When - without filtering
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, new User()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, new User()));

        // Then - without filtering
        assertNotNull(resultsWithoutFiltering);
        assertEquals(9, resultsWithoutFiltering.size());
        ElementUtil.assertElementEquals(resultsWithoutFiltering, Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),

                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),

                getEntities().get(new EntitySeed("A3"))));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(3, resultsWithFiltering.size());
        ElementUtil.assertElementEquals(resultsWithFiltering, Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEntities().get(new EntitySeed("A3"))));
    }

    @GafferTest
    @TraitRequirement({StoreTrait.POST_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    public void testPostAggregationFilteringProperties(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final List<ElementId> seeds = Arrays.asList(new EntitySeed("A3"),
                new EdgeSeed("A5", "B5", false));

        final GetElements getElementsWithoutFiltering =
                new GetElements.Builder()
                        .input(seeds)
                        .build();

        final GetElements getElementsWithFiltering = new GetElements.Builder()
                .input(seeds)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.VERTEX.name())
                                        .execute(new IsEqual("A5"))
                                        .build())
                                .build())
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.INT)
                                        .execute(new IsLessThan(2))
                                        .build())
                                .build())
                        .build())
                .build();

        // When - without filtering
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, new User()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, new User()));

        // Then - without filtering
        List<Element> expectedResults = Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),
                getEdges().get(new EdgeSeed("A5", "B5", false)),

                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),

                getEntities().get(new EntitySeed("A3")),
                getEntities().get(new EntitySeed("A5")),
                getEntities().get(new EntitySeed("B5"))
        );

        ElementUtil.assertElementEquals(expectedResults, resultsWithoutFiltering);

        // Then - with filtering
        List<Element> expectedFilteredResults = Arrays.asList(
                getEdges().get(new EdgeSeed("A3", "A3", false)),
                getEdges().get(new EdgeSeed("A3", "B3", false)),
                getEdges().get(new EdgeSeed("A5", "B5", false)),
                getEdges().get(new EdgeSeed("A3", "D3", false)),
                getEdges().get(new EdgeSeed("A3", "C3", false)),

                getEdges().get(new EdgeSeed("A3", "A3", true)),
                getEdges().get(new EdgeSeed("A3", "B3", true)),
                getEdges().get(new EdgeSeed("A3", "C3", true)),
                getEdges().get(new EdgeSeed("A3", "D3", true)),

                getEntities().get(new EntitySeed("A5"))
        );
        ElementUtil.assertElementEquals(expectedFilteredResults, resultsWithFiltering);
    }

}
