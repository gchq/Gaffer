/*
 * Copyright 2016 Crown Copyright
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
package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsEqual;
import gaffer.function.simple.filter.IsLessThan;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreTrait;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FilteringIT extends AbstractStoreIT {
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringGroups() throws OperationException {
        // Given
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) new EntitySeed("A3"));

        final GetRelatedElements<ElementSeed, Element> getElementsWithoutFiltering =
                new GetRelatedElements.Builder<>()
                        .seeds(seeds)
                        .build();

        final GetRelatedElements<ElementSeed, Element> getElementsWithFiltering = new GetRelatedElements.Builder<>()
                .seeds(seeds)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        // When - without filtering
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        assertNotNull(resultsWithoutFiltering);
        assertEquals(5, resultsWithoutFiltering.size());
        assertThat(resultsWithoutFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEntity("A3")
        ));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(1, resultsWithFiltering.size());
        assertThat(resultsWithFiltering, IsCollectionContaining.hasItems(
                (Element) getEntity("A3")
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringIdentifiers() throws OperationException {
        // Given
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) new EntitySeed("A3"));

        final GetRelatedElements<ElementSeed, Element> getElementsWithoutFiltering =
                new GetRelatedElements.Builder<>()
                        .seeds(seeds)
                        .build();

        final GetRelatedElements<ElementSeed, Element> getElementsWithFiltering = new GetRelatedElements.Builder<>()
                .seeds(seeds)
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
        assertNotNull(resultsWithoutFiltering);
        assertEquals(5, resultsWithoutFiltering.size());
        assertThat(resultsWithoutFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEntity("A3")
        ));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(2, resultsWithFiltering.size());
        assertThat(resultsWithFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "B3", false),
                getEntity("A3")
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    public void testFilteringProperties() throws OperationException {
        // Given
        final List<ElementSeed> seeds = Arrays.asList(new EntitySeed("A3"),
                new EdgeSeed("A5", "B5", false));

        final GetRelatedElements<ElementSeed, Element> getElementsWithoutFiltering =
                new GetRelatedElements.Builder<>()
                        .seeds(seeds)
                        .build();

        final GetRelatedElements<ElementSeed, Element> getElementsWithFiltering = new GetRelatedElements.Builder<>()
                .seeds(seeds)
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
        final List<Element> resultsWithoutFiltering = Lists.newArrayList(graph.execute(getElementsWithoutFiltering, getUser()));

        // When - with filtering
        final List<Element> resultsWithFiltering = Lists.newArrayList(graph.execute(getElementsWithFiltering, getUser()));

        // Then - without filtering
        assertNotNull(resultsWithoutFiltering);
        assertEquals(8, resultsWithoutFiltering.size());
        assertThat(resultsWithoutFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEdge("A5", "B5", false),
                getEntity("A5"),
                getEntity("B5")
        ));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(6, resultsWithFiltering.size());
        assertThat(resultsWithFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A5", "B5", false),
                getEdge("A3", "D3", false),
                getEdge("A3", "C3", false),
                getEntity("A5")
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void testPostAggregationFilteringIdentifiers() throws OperationException {
        // Given
        final List<ElementSeed> seeds = Collections.singletonList((ElementSeed) new EntitySeed("A3"));

        final GetRelatedElements<ElementSeed, Element> getElementsWithoutFiltering =
                new GetRelatedElements.Builder<>()
                        .seeds(seeds)
                        .build();

        final GetRelatedElements<ElementSeed, Element> getElementsWithFiltering = new GetRelatedElements.Builder<>()
                .seeds(seeds)
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
        assertNotNull(resultsWithoutFiltering);
        assertEquals(5, resultsWithoutFiltering.size());
        assertThat(resultsWithoutFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEntity("A3")
        ));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(2, resultsWithFiltering.size());
        assertThat(resultsWithFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "B3", false),
                getEntity("A3")
        ));
    }

    @Test
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void testPostAggregationFilteringProperties() throws OperationException {
        // Given
        final List<ElementSeed> seeds = Arrays.asList(new EntitySeed("A3"),
                new EdgeSeed("A5", "B5", false));

        final GetRelatedElements<ElementSeed, Element> getElementsWithoutFiltering =
                new GetRelatedElements.Builder<>()
                        .seeds(seeds)
                        .build();

        final GetRelatedElements<ElementSeed, Element> getElementsWithFiltering = new GetRelatedElements.Builder<>()
                .seeds(seeds)
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
        assertNotNull(resultsWithoutFiltering);
        assertEquals(8, resultsWithoutFiltering.size());
        assertThat(resultsWithoutFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A3", "C3", false),
                getEdge("A3", "D3", false),
                getEdge("A5", "B5", false),
                getEntity("A5"),
                getEntity("B5")
        ));

        // Then - with filtering
        assertNotNull(resultsWithFiltering);
        assertEquals(6, resultsWithFiltering.size());
        assertThat(resultsWithFiltering, IsCollectionContaining.hasItems(
                getEdge("A3", "A3", false),
                getEdge("A3", "B3", false),
                getEdge("A5", "B5", false),
                getEdge("A3", "D3", false),
                getEdge("A3", "C3", false),
                getEntity("A5")
        ));
    }


}
