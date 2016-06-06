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
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.Entity;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsEqual;
import gaffer.function.simple.transform.Concat;
import gaffer.integration.AbstractStoreIT;
import gaffer.integration.TraitRequirement;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.StoreTrait;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetAllElementsIT extends AbstractStoreIT {
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldGetAllElements() throws Exception {
        for (boolean includeEntities : Arrays.asList(true, false)) {
            for (IncludeEdgeType includeEdgeType : IncludeEdgeType.values()) {
                if (!includeEntities && IncludeEdgeType.NONE == includeEdgeType) {
                    // Cannot query for nothing!
                    continue;
                }
                try {
                    shouldGetAllElements(includeEntities, includeEdgeType);
                } catch (AssertionError e) {
                    throw new AssertionError("GetAllElements failed with parameters: includeEntities=" + includeEntities
                            + ", includeEdgeType=" + includeEdgeType.name(), e);
                }
            }
        }
    }

    @TraitRequirement(StoreTrait.FILTERING)
    @Test
    public void shouldGetAllFilteredElements() throws Exception {
        final GetAllElements<Element> op = new GetAllElements.Builder<>()
                .populateProperties(true)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select(IdentifierType.VERTEX)
                                        .execute(new IsEqual("A1"))
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertEquals("A1", ((Entity) resultList.get(0)).getVertex());
    }

    @TraitRequirement({StoreTrait.TRANSFORMATION, StoreTrait.FILTERING})
    @Test
    public void shouldGetAllTransformedFilteredElements() throws Exception {
        final GetAllElements<Element> op = new GetAllElements.Builder<>()
                .populateProperties(true)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select(IdentifierType.VERTEX)
                                        .execute(new IsEqual("A1"))
                                        .build())
                                .transientProperty(TestPropertyNames.TRANSIENT_1, String.class)
                                .transformer(new ElementTransformer.Builder()
                                        .select(new ElementComponentKey(IdentifierType.VERTEX),
                                                new ElementComponentKey(TestPropertyNames.STRING))
                                        .project(TestPropertyNames.TRANSIENT_1)
                                        .execute(new Concat())
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        final List<Element> resultList = Lists.newArrayList(results);
        assertEquals(1, resultList.size());
        assertEquals("A1,3", resultList.get(0).getProperties().get(TestPropertyNames.TRANSIENT_1));
    }

    protected void shouldGetAllElements(boolean includeEntities, final IncludeEdgeType includeEdgeType) throws Exception {
        // Given
        final List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(getEntities().values());
        }

        for (Edge edge : getEdges().values()) {
            if (IncludeEdgeType.ALL == includeEdgeType
                    || (edge.isDirected() && IncludeEdgeType.DIRECTED == includeEdgeType)
                    || (!edge.isDirected() && IncludeEdgeType.UNDIRECTED == includeEdgeType)) {
                expectedElements.add(edge);
            }
        }

        final GetAllElements<Element> op = new GetAllElements.Builder<>()
                .includeEntities(includeEntities)
                .includeEdges(includeEdgeType)
                .populateProperties(true)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        final List<Element> expectedElementsCopy = Lists.newArrayList(expectedElements);
        for (Element result : results) {
            final ElementSeed seed = ElementSeed.createSeed(result);
            if (result instanceof Entity) {
                Entity entity = (Entity) result;
                assertTrue("Entity was not expected: " + entity, expectedElements.contains(entity));
            } else {
                Edge edge = (Edge) result;
                if (edge.isDirected()) {
                    assertTrue("Edge was not expected: " + edge, expectedElements.contains(edge));
                } else {
                    final Edge edgeReversed = new Edge(TestGroups.EDGE, edge.getDestination(), edge.getSource(), edge.isDirected());
                    expectedElementsCopy.remove(edgeReversed);
                    assertTrue("Edge was not expected: " + seed, expectedElements.contains(result) || expectedElements.contains(edgeReversed));
                }
            }
            expectedElementsCopy.remove(result);
        }

        assertEquals("The number of elements returned was not as expected. Missing elements: " + expectedElementsCopy, expectedElements.size(),
                Lists.newArrayList(results).size());
    }
}