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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters.DirectedType;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;

public class GetElementsIT extends AbstractStoreIT {
    // ElementId Seeds
    public static final List<ElementId> ENTITY_SEEDS_EXIST =
            Arrays.asList(
                    (ElementId) new EntitySeed(SOURCE_2),
                    new EntitySeed(DEST_3),
                    new EntitySeed(SOURCE_DIR_2),
                    new EntitySeed(DEST_DIR_3));

    public static final List<Element> ENTITIES_EXIST =
            getElements(ENTITY_SEEDS_EXIST);

    public static final List<ElementId> EDGE_SEEDS_EXIST =
            Collections.singletonList(
                    (ElementId) new EdgeSeed(SOURCE_1, DEST_1, false));

    public static final List<Element> EDGES_EXIST =
            getElements(EDGE_SEEDS_EXIST);

    public static final List<ElementId> EDGE_DIR_SEEDS_EXIST =
            Collections.singletonList(
                    (ElementId) new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true));

    public static final List<Element> EDGES_DIR_EXIST =
            getElements(EDGE_DIR_SEEDS_EXIST);

    public static final List<ElementId> EDGE_SEEDS_DONT_EXIST =
            Arrays.asList(
                    (ElementId) new EdgeSeed(SOURCE_1, "dest2DoesNotExist", false),
                    new EdgeSeed("source2DoesNotExist", DEST_1, false),
                    new EdgeSeed(SOURCE_1, DEST_1, true));// does not exist

    public static final List<ElementId> ENTITY_SEEDS_DONT_EXIST =
            Collections.singletonList(
                    (ElementId) new EntitySeed("idDoesNotExist"));

    public static final List<ElementId> ENTITY_SEEDS = getEntityIds();
    public static final List<ElementId> EDGE_SEEDS = getEdgeIds();
    public static final List<ElementId> ALL_SEEDS = getAllSeeds();
    public static final List<Object> ALL_SEED_VERTICES = getAllSeededVertices();


    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldGetElements() throws Exception {
        final List<DirectedType> directedTypes = Lists.newArrayList(DirectedType.values());
        directedTypes.add(null);

        final List<IncludeIncomingOutgoingType> inOutTypes = Lists.newArrayList(IncludeIncomingOutgoingType.values());
        inOutTypes.add(null);

        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final boolean includeEdges : Arrays.asList(true, false)) {
                if (!includeEntities && !includeEdges) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final DirectedType directedType : directedTypes) {
                    for (final IncludeIncomingOutgoingType inOutType : inOutTypes) {
                        try {
                            shouldGetElementsBySeed(includeEntities, includeEdges, directedType, inOutType);
                        } catch (final AssertionError e) {
                            throw new AssertionError("GetElementsBySeed failed with parameters: includeEntities=" + includeEntities
                                    + ", includeEdges=" + includeEdges + ", directedType=" + directedType + ", inOutType=" + inOutType, e);
                        }

                        try {
                            shouldGetRelatedElements(includeEntities, includeEdges, directedType, inOutType);
                        } catch (final AssertionError e) {
                            throw new AssertionError("GetRelatedElements failed with parameters: includeEntities=" + includeEntities
                                    + ", includeEdges=" + includeEdges + ", directedType=" + directedType + ", inOutType=" + inOutType, e);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetElementsBySeed() throws Exception {
        // Given
        final GetElements op = new GetElements();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertFalse(results.iterator().hasNext());
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetRelatedElements() throws Exception {
        // Given
        final GetElements op = new GetElements();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertFalse(results.iterator().hasNext());
    }

    private void shouldGetElementsBySeed(final boolean includeEntities,
                                         final boolean includeEdges,
                                         final DirectedType directedType,
                                         final IncludeIncomingOutgoingType inOutType) throws Exception {
        final List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(ENTITIES_EXIST);
        }

        if (includeEdges) {
            if (DirectedType.UNDIRECTED != directedType) {
                expectedElements.addAll(EDGES_DIR_EXIST);
            }
            if (DirectedType.DIRECTED != directedType) {
                expectedElements.addAll(EDGES_EXIST);
            }
        }

        final List<ElementId> seeds;
        if (includeEdges) {
            if (includeEntities) {
                seeds = ALL_SEEDS;
            } else {
                seeds = EDGE_SEEDS;
            }
        } else if (includeEntities) {
            seeds = ENTITY_SEEDS;
        } else {
            seeds = new ArrayList<>();
        }

        shouldGetElements(expectedElements, SeedMatchingType.EQUAL, directedType, includeEntities, includeEdges, inOutType, seeds);
    }


    private void shouldGetRelatedElements(final boolean includeEntities,
                                          final boolean includeEdges,
                                          final DirectedType directedType,
                                          final IncludeIncomingOutgoingType inOutType) throws Exception {
        final List<ElementId> seedTerms = new LinkedList<>();
        final List<Element> expectedElements = new LinkedList<>();
        if (includeEntities) {
            for (final Object identifier : ALL_SEED_VERTICES) {
                final EntityId entityId = new EntitySeed(identifier);
                seedTerms.add(entityId);
            }
        }

        if (includeEdges) {
            if (DirectedType.UNDIRECTED != directedType) {
                final EdgeId seed = new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true);
                seedTerms.add(seed);

                if (null == inOutType || IncludeIncomingOutgoingType.BOTH == inOutType || IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                    final EdgeId seedSourceDestDir2 = new EdgeSeed(SOURCE_DIR_2, DEST_DIR_2, true);
                    seedTerms.add(seedSourceDestDir2);
                }

                if (null == inOutType || IncludeIncomingOutgoingType.BOTH == inOutType || IncludeIncomingOutgoingType.INCOMING == inOutType) {
                    final EdgeId seedSourceDestDir3 = new EdgeSeed(SOURCE_DIR_3, DEST_DIR_3, true);
                    seedTerms.add(seedSourceDestDir3);
                }
            }

            if (DirectedType.DIRECTED != directedType) {
                final EdgeId seedSourceDest1 = new EdgeSeed(SOURCE_1, DEST_1, false);
                seedTerms.add(seedSourceDest1);

                final EdgeId seedSourceDest2 = new EdgeSeed(SOURCE_2, DEST_2, false);
                seedTerms.add(seedSourceDest2);

                final EdgeId seedSourceDest3 = new EdgeSeed(SOURCE_3, DEST_3, false);
                seedTerms.add(seedSourceDest3);
            }
        }

        expectedElements.addAll(getElements(seedTerms));
        shouldGetElements(expectedElements, SeedMatchingType.RELATED, directedType, includeEntities, includeEdges, inOutType, ALL_SEEDS);
    }

    private void shouldGetElements(final List<Element> expectedElements,
                                   final SeedMatchingType seedMatching,
                                   final DirectedType directedType,
                                   final boolean includeEntities,
                                   final boolean includeEdges,
                                   final IncludeIncomingOutgoingType inOutType,
                                   final Iterable<ElementId> seeds) throws IOException, OperationException {
        // Given
        final User user = new User();

        final View.Builder viewBuilder = new View.Builder();
        if (includeEntities) {
            viewBuilder.entity(TestGroups.ENTITY);
        }
        if (includeEdges) {
            viewBuilder.edge(TestGroups.EDGE);
        }

        final GetElements op = new GetElements.Builder()
                .input(seeds)
                .directedType(directedType)
                .inOutType(inOutType)
                .view(viewBuilder.build())
                .seedMatching(seedMatching).build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        final List<Element> expectedElementsCopy = Lists.newArrayList(expectedElements);
        for (final Element result : results) {
            if (result instanceof Entity) {
                Entity entity = (Entity) result;

                final Collection<Element> listOfElements = new LinkedList<>();
                Iterables.addAll(listOfElements, results);

                assertTrue("Entity was not expected: " + entity, expectedElements.contains(entity));
            } else {
                Edge edge = (Edge) result;
                if (edge.isDirected()) {
                    assertTrue("Edge was not expected: " + edge, expectedElements.contains(edge));
                } else {
                    final Edge edgeReversed = new Edge(TestGroups.EDGE, edge.getDestination(), edge.getSource(), edge.isDirected());

                    Properties properties = edge.getProperties();
                    edgeReversed.copyProperties(properties);

                    expectedElementsCopy.remove(edgeReversed);
                    assertTrue("Edge was not expected: " + result, expectedElements.contains(result) || expectedElements.contains(edgeReversed));
                }
            }
            expectedElementsCopy.remove(result);
        }

        assertEquals("The number of elements returned was not as expected. Missing elements: " + expectedElementsCopy + ". Seeds: " + seeds, expectedElements.size(),
                Sets.newHashSet(results).size());

        assertEquals(new HashSet<>(expectedElements), Sets.newHashSet(results));
    }

    private static List<Element> getElements(final List<ElementId> seeds) {
        final List<Element> elements = new ArrayList<>(seeds.size());
        for (final ElementId seed : seeds) {
            if (seed instanceof EntityId) {
                final Entity entity = new Entity(TestGroups.ENTITY, ((EntityId) seed).getVertex());
                entity.putProperty("stringProperty", "3");
                elements.add(entity);
            } else {
                final Edge edge = new Edge(TestGroups.EDGE, ((EdgeId) seed).getSource(), ((EdgeId) seed).getDestination(), ((EdgeId) seed).isDirected());
                edge.putProperty("intProperty", 1);
                edge.putProperty("count", 1L);
                elements.add(edge);
            }
        }

        return elements;
    }

    private static List<ElementId> getEntityIds() {
        List<ElementId> allSeeds = new ArrayList<>();
        allSeeds.addAll(ENTITY_SEEDS_EXIST);
        allSeeds.addAll(ENTITY_SEEDS_DONT_EXIST);
        return allSeeds;
    }

    private static List<ElementId> getEdgeIds() {
        List<ElementId> allSeeds = new ArrayList<>();
        allSeeds.addAll(EDGE_SEEDS_EXIST);
        allSeeds.addAll(EDGE_DIR_SEEDS_EXIST);
        allSeeds.addAll(EDGE_SEEDS_DONT_EXIST);
        return allSeeds;
    }

    private static List<ElementId> getAllSeeds() {
        List<ElementId> allSeeds = new ArrayList<>();
        allSeeds.addAll(ENTITY_SEEDS);
        allSeeds.addAll(EDGE_SEEDS);
        return allSeeds;
    }

    private static List<Object> getAllSeededVertices() {
        List<Object> allSeededVertices = new ArrayList<>();
        for (final ElementId elementId : ENTITY_SEEDS_EXIST) {
            allSeededVertices.add(((EntityId) elementId).getVertex());
        }

        for (final ElementId elementId : EDGE_SEEDS_EXIST) {
            allSeededVertices.add(((EdgeId) elementId).getSource());
            allSeededVertices.add(((EdgeId) elementId).getDestination());
        }

        for (final ElementId elementId : EDGE_DIR_SEEDS_EXIST) {
            allSeededVertices.add(((EdgeId) elementId).getSource());
            allSeededVertices.add(((EdgeId) elementId).getDestination());
        }

        return allSeededVertices;
    }
}