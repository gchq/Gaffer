/*
 * Copyright 2016-2023 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FilteringIT extends AbstractStoreIT {

    @Override
    public void _setup() throws Exception {
        addDefaultElements();
    }

    @Test
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringGroups() throws OperationException {
        // Given
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
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        assertThat(resultsWithoutFiltering)
                .hasSize(9);
        ElementUtil.assertElementEquals(resultsWithoutFiltering, Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),

                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),

                getEntity("A3")));

        // Then - with filtering
        assertThat(resultsWithFiltering)
                .hasSize(1);
        ElementUtil.assertElementEquals(resultsWithFiltering, Arrays.asList(
                (Element) getEntity("A3")
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringIdentifiers() throws OperationException {
        // Given
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
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        assertThat(resultsWithoutFiltering)
                .hasSize(9);
        ElementUtil.assertElementEquals(resultsWithoutFiltering, Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),

                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),

                getEntity("A3")));

        // Then - with filtering
        assertThat(resultsWithFiltering)
                .hasSize(3);
        ElementUtil.assertElementEquals(resultsWithFiltering, Arrays.asList(
                getEdge("A3", "B3", false),
                getEdge("A3", "B3", true),
                getEntity("A3")));
    }

    @Test
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringProperties() throws OperationException {
        // Given
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
                graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(
                graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        List<Element> expectedResults = Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEdge("A5", "B5", false),

                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),

                getEntity("A3"),
                getEntity("A5"),
                getEntity("B5")
        );
        ElementUtil.assertElementEquals(expectedResults, resultsWithoutFiltering);

        // Then - with filtering
        List<Element> expectedFilteredResults = Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "D3", false),
                getEdge("A3", "C3", false),
                getEdge("A5", "B5", false),
                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),
                getEntity("A5")
        );
        ElementUtil.assertElementEquals(expectedFilteredResults, resultsWithFiltering);
    }

    @Test
    @TraitRequirement({StoreTrait.POST_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    public void testPostAggregationFilteringIdentifiers() throws OperationException {
        // Given
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
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        assertThat(resultsWithoutFiltering)
                .hasSize(9);
        ElementUtil.assertElementEquals(resultsWithoutFiltering, Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),

                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),

                getEntity("A3")));

        // Then - with filtering
        assertThat(resultsWithFiltering)
                .hasSize(3);
        ElementUtil.assertElementEquals(resultsWithFiltering, Arrays.asList(
                getEdge("A3", "B3", false),
                getEdge("A3", "B3", true),
                getEntity("A3")));
    }

    @Test
    @TraitRequirement({StoreTrait.POST_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    public void testPostAggregationFilteringProperties() throws OperationException {
        // Given
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
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        List<Element> expectedResults = Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEdge("A5", "B5", false),

                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),

                getEntity("A3"),
                getEntity("A5"),
                getEntity("B5")
        );

        ElementUtil.assertElementEquals(expectedResults, resultsWithoutFiltering);

        // Then - with filtering
        List<Element> expectedFilteredResults = Arrays.asList(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A5", "B5", false),
                getEdge("A3", "D3", false),
                getEdge("A3", "C3", false),

                getEdge("A3", "A3", true),
                getEdge("A3", "B3", true),
                getEdge("A3", "C3", true),
                getEdge("A3", "D3", true),

                getEntity("A5")
        );
        ElementUtil.assertElementEquals(expectedFilteredResults, resultsWithFiltering);
    }

}
