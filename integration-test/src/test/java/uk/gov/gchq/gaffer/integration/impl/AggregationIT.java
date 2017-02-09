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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AggregationIT extends AbstractStoreIT {
    private final String AGGREGATED_ID = "3,3";
    private final String AGGREGATED_SOURCE = SOURCE + 6;
    private final String AGGREGATED_DEST = DEST + 6;

    private final int NON_AGGREGATED_ID = 8;
    private final String NON_AGGREGATED_SOURCE = SOURCE + NON_AGGREGATED_ID;
    private final String NON_AGGREGATED_DEST = DEST + NON_AGGREGATED_ID;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();

        // Add duplicate elements
        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(getEntity(AGGREGATED_SOURCE)))
                .build(), getUser());

        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(getEdge(AGGREGATED_SOURCE, AGGREGATED_DEST, false)))
                .build(), getUser());

        // Edge with existing ids but directed
        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(new Edge(TestGroups.EDGE, NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, true)))
                .build(), getUser());
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldAggregateIdenticalElements() throws OperationException, UnsupportedEncodingException {
        // Given
        final GetElements<ElementSeed, Element> getElements = new GetElements.Builder<>()
                .addSeed(new EntitySeed(AGGREGATED_SOURCE))
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements, getUser()));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());

        final Entity expectedEntity = new Entity(TestGroups.ENTITY, AGGREGATED_SOURCE);
        expectedEntity.putProperty(TestPropertyNames.STRING, "3,3");

        final Edge expectedEdge = new Edge(TestGroups.EDGE, AGGREGATED_SOURCE, AGGREGATED_DEST, false);
        expectedEdge.putProperty(TestPropertyNames.INT, 1);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 2L);

        assertThat(results, IsCollectionContaining.hasItems(
                expectedEdge,
                expectedEntity
        ));

        for (final Element result : results) {
            if (result instanceof Entity) {
                assertEquals(AGGREGATED_ID, result.getProperty(TestPropertyNames.STRING));
            } else {
                assertEquals(1, result.getProperty(TestPropertyNames.INT));
                assertEquals(2L, result.getProperty(TestPropertyNames.COUNT));
            }
        }
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldNotAggregateEdgesWithDifferentDirectionFlag() throws OperationException {
        // Given
        final GetEdges<EntitySeed> getEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed(NON_AGGREGATED_SOURCE))
                .build();

        // When
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges, getUser()));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                getEdge(NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, false),
                new Edge(TestGroups.EDGE, NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, true)
        ));
    }
}
