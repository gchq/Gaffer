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
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreTrait;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

public class AggregationIT extends AbstractStoreIT {
    private final int AGGREGATED_ID = 6;
    private final String AGGREGATED_SOURCE = SOURCE + AGGREGATED_ID;
    private final String AGGREGATED_DEST = DEST + AGGREGATED_ID;

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
                .build());

        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(getEdge(AGGREGATED_SOURCE, AGGREGATED_DEST, false)))
                .build());

        // Edge with existing ids but directed
        graph.execute(new AddElements.Builder()
                .elements(Collections.<Element>singleton(new Edge(TestGroups.EDGE, NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, true)))
                .build());
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldAggregateIdenticalElements() throws OperationException, UnsupportedEncodingException {
        // Given
        final GetRelatedElements<ElementSeed, Element> getElements = new GetRelatedElements.Builder<>()
                .addSeed(new EntitySeed(AGGREGATED_SOURCE))
                .build();

        // When
        final List<Element> results = Lists.newArrayList(graph.execute(getElements));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                getEdge(AGGREGATED_SOURCE, AGGREGATED_DEST, false),
                getEntity(AGGREGATED_SOURCE)
        ));

        for (Element result : results) {
            if (result instanceof Entity) {
                assertEquals(AGGREGATED_ID + "," + AGGREGATED_ID, result.getProperty(TestPropertyNames.STRING));
            } else {
                assertEquals(6, result.getProperty(TestPropertyNames.INT));
                assertEquals(2L, result.getProperty(TestPropertyNames.COUNT));
            }
        }
    }

    @Test
    @TraitRequirement(StoreTrait.AGGREGATION)
    public void shouldNotAggregateEdgesWithDifferentDirectionFlag() throws OperationException {
        // Given
        final GetRelatedEdges getEdges = new GetRelatedEdges.Builder()
                .addSeed(new EntitySeed(NON_AGGREGATED_SOURCE))
                .build();

        // When
        final List<Edge> results = Lists.newArrayList(graph.execute(getEdges));

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        assertThat(results, IsCollectionContaining.hasItems(
                getEdge(NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, false),
                new Edge(TestGroups.EDGE, NON_AGGREGATED_SOURCE, NON_AGGREGATED_DEST, true)
        ));
    }
}
