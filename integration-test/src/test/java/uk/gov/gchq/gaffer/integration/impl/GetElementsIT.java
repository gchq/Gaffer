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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
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

public class GetElementsIT extends AbstractStoreIT {
    // ElementSeed Seeds
    public static final List<ElementSeed> ENTITY_SEEDS_EXIST =
            Arrays.asList(
                    (ElementSeed) new EntitySeed(SOURCE_2),
                    new EntitySeed(DEST_3),
                    new EntitySeed(SOURCE_DIR_2),
                    new EntitySeed(DEST_DIR_3));

    public static final List<Element> ENTITIES_EXIST =
            getElements(ENTITY_SEEDS_EXIST);

    public static final List<ElementSeed> EDGE_SEEDS_EXIST =
            Collections.singletonList(
                    (ElementSeed) new EdgeSeed(SOURCE_1, DEST_1, false));

    public static final List<Element> EDGES_EXIST =
            getElements(EDGE_SEEDS_EXIST);

    public static final List<ElementSeed> EDGE_DIR_SEEDS_EXIST =
            Collections.singletonList(
                    (ElementSeed) new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true));

    public static final List<Element> EDGES_DIR_EXIST =
            getElements(EDGE_DIR_SEEDS_EXIST);

    public static final List<ElementSeed> EDGE_SEEDS_DONT_EXIST =
            Arrays.asList(
                    (ElementSeed) new EdgeSeed(SOURCE_1, "dest2DoesNotExist", false),
                    new EdgeSeed("source2DoesNotExist", DEST_1, false),
                    new EdgeSeed(SOURCE_1, DEST_1, true));// does not exist

    public static final List<ElementSeed> ENTITY_SEEDS_DONT_EXIST =
            Collections.singletonList(
                    (ElementSeed) new EntitySeed("idDoesNotExist"));

    public static final List<ElementSeed> ENTITY_SEEDS = getEntitySeeds();
    public static final List<ElementSeed> EDGE_SEEDS = getEdgeSeeds();
    public static final List<ElementSeed> ALL_SEEDS = getAllSeeds();
    public static final List<Object> ALL_SEED_VERTICES = getAllSeededVertices();


    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldGetElements() throws Exception {
        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final IncludeEdgeType includeEdgeType : IncludeEdgeType.values()) {
                if (!includeEntities && IncludeEdgeType.NONE == includeEdgeType) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final IncludeIncomingOutgoingType inOutType : IncludeIncomingOutgoingType.values()) {
                    try {
                        shouldGetElementsBySeed(includeEntities, includeEdgeType, inOutType);
                    } catch (AssertionError e) {
                        throw new AssertionError("GetElementsBySeed failed with parameters: includeEntities=" + includeEntities
                                + ", includeEdgeType=" + includeEdgeType.name() + ", inOutType=" + inOutType, e);
                    }

                    try {
                        shouldGetRelatedElements(includeEntities, includeEdgeType, inOutType);
                    } catch (AssertionError e) {
                        throw new AssertionError("GetRelatedElements failed with parameters: includeEntities=" + includeEntities
                                + ", includeEdgeType=" + includeEdgeType.name() + ", inOutType=" + inOutType, e);
                    }
                }
            }
        }
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetElementsBySeed() throws Exception {
        // Given
        final GetElements<ElementSeed, Element> op = new GetElements<>();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertFalse(results.iterator().hasNext());
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetRelatedElements() throws Exception {
        // Given
        final GetElements<ElementSeed, Element> op = new GetElements<>();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertFalse(results.iterator().hasNext());
    }

    private void shouldGetElementsBySeed(boolean includeEntities, final IncludeEdgeType includeEdgeType, final IncludeIncomingOutgoingType inOutType) throws Exception {
        final List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(ENTITIES_EXIST);
        }
        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.DIRECTED == includeEdgeType) {
            expectedElements.addAll(EDGES_DIR_EXIST);
        }
        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.UNDIRECTED == includeEdgeType) {
            expectedElements.addAll(EDGES_EXIST);
        }

        final List<ElementSeed> seeds;
        if (IncludeEdgeType.NONE != includeEdgeType) {
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

        shouldGetElements(expectedElements, SeedMatchingType.EQUAL, includeEdgeType, includeEntities, inOutType, seeds);
    }


    private void shouldGetRelatedElements(boolean includeEntities, final IncludeEdgeType includeEdgeType, final IncludeIncomingOutgoingType inOutType) throws Exception {
        final List<ElementSeed> seedTerms = new LinkedList<>();
        final List<Element> expectedElements = new LinkedList<>();
        if (includeEntities) {
            for (final Object identifier : ALL_SEED_VERTICES) {
                final EntitySeed entitySeed = new EntitySeed(identifier);
                seedTerms.add(entitySeed);
            }
        }

        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.DIRECTED == includeEdgeType) {
            final EdgeSeed seed = new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true);
            seedTerms.add(seed);

            if (IncludeIncomingOutgoingType.BOTH == inOutType || IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                final EdgeSeed seedSourceDestDir2 = new EdgeSeed(SOURCE_DIR_2, DEST_DIR_2, true);
                seedTerms.add(seedSourceDestDir2);
            }

            if (IncludeIncomingOutgoingType.BOTH == inOutType || IncludeIncomingOutgoingType.INCOMING == inOutType) {
                final EdgeSeed seedSourceDestDir3 = new EdgeSeed(SOURCE_DIR_3, DEST_DIR_3, true);
                seedTerms.add(seedSourceDestDir3);
            }
        }

        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.UNDIRECTED == includeEdgeType) {
            final EdgeSeed seedSourceDest1 = new EdgeSeed(SOURCE_1, DEST_1, false);
            seedTerms.add(seedSourceDest1);

            final EdgeSeed seedSourceDest2 = new EdgeSeed(SOURCE_2, DEST_2, false);
            seedTerms.add(seedSourceDest2);

            final EdgeSeed seedSourceDest3 = new EdgeSeed(SOURCE_3, DEST_3, false);
            seedTerms.add(seedSourceDest3);
        }

        expectedElements.addAll(getElements(seedTerms));
        shouldGetElements(expectedElements, SeedMatchingType.RELATED, includeEdgeType, includeEntities, inOutType, ALL_SEEDS);
    }

    private void shouldGetElements(final List<Element> expectedElements, final SeedMatchingType seedMatching,
                                   final IncludeEdgeType includeEdgeType,
                                   final Boolean includeEntities,
                                   final IncludeIncomingOutgoingType inOutType,
                                   final Iterable<ElementSeed> seeds) throws IOException, OperationException {
        // Given
        final User user = new User();
        final GetElements<ElementSeed, Element> op = new GetElements.Builder<>().seedMatching(seedMatching).build();
        op.setSeeds(seeds);
        op.setIncludeEntities(includeEntities);
        op.setIncludeEdges(includeEdgeType);
        op.setIncludeIncomingOutGoing(inOutType);
        op.setView(new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build());


        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        final List<Element> expectedElementsCopy = Lists.newArrayList(expectedElements);
        for (final Element result : results) {
            final ElementSeed seed = ElementSeed.createSeed(result);
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
                    assertTrue("Edge was not expected: " + seed, expectedElements.contains(result) || expectedElements.contains(edgeReversed));
                }
            }
            expectedElementsCopy.remove(result);
        }

        assertEquals("The number of elements returned was not as expected. Missing elements: " + expectedElementsCopy + ". Seeds: " + seeds, expectedElements.size(),
                Sets.newHashSet(results).size());

        assertEquals(new HashSet<>(expectedElements), Sets.newHashSet(results));
    }

    private static List<Element> getElements(final List<ElementSeed> seeds) {
        final List<Element> elements = new ArrayList<>(seeds.size());
        for (final ElementSeed seed : seeds) {
            if (seed instanceof EntitySeed) {
                final Entity entity = new Entity(TestGroups.ENTITY, ((EntitySeed) seed).getVertex());
                entity.putProperty("stringProperty", "3");
                elements.add(entity);
            } else {
                final Edge edge = new Edge(TestGroups.EDGE, ((EdgeSeed) seed).getSource(), ((EdgeSeed) seed).getDestination(), ((EdgeSeed) seed).isDirected());
                edge.putProperty("intProperty", 1);
                edge.putProperty("count", 1L);
                elements.add(edge);
            }
        }

        return elements;
    }

    private static List<ElementSeed> getEntitySeeds() {
        List<ElementSeed> allSeeds = new ArrayList<>();
        allSeeds.addAll(ENTITY_SEEDS_EXIST);
        allSeeds.addAll(ENTITY_SEEDS_DONT_EXIST);
        return allSeeds;
    }

    private static List<ElementSeed> getEdgeSeeds() {
        List<ElementSeed> allSeeds = new ArrayList<>();
        allSeeds.addAll(EDGE_SEEDS_EXIST);
        allSeeds.addAll(EDGE_DIR_SEEDS_EXIST);
        allSeeds.addAll(EDGE_SEEDS_DONT_EXIST);
        return allSeeds;
    }

    private static List<ElementSeed> getAllSeeds() {
        List<ElementSeed> allSeeds = new ArrayList<>();
        allSeeds.addAll(ENTITY_SEEDS);
        allSeeds.addAll(EDGE_SEEDS);
        return allSeeds;
    }

    private static List<Object> getAllSeededVertices() {
        List<Object> allSeededVertices = new ArrayList<>();
        for (final ElementSeed elementSeed : ENTITY_SEEDS_EXIST) {
            allSeededVertices.add(((EntitySeed) elementSeed).getVertex());
        }

        for (final ElementSeed elementSeed : EDGE_SEEDS_EXIST) {
            allSeededVertices.add(((EdgeSeed) elementSeed).getSource());
            allSeededVertices.add(((EdgeSeed) elementSeed).getDestination());
        }

        for (final ElementSeed elementSeed : EDGE_DIR_SEEDS_EXIST) {
            allSeededVertices.add(((EdgeSeed) elementSeed).getSource());
            allSeededVertices.add(((EdgeSeed) elementSeed).getDestination());
        }

        return allSeededVertices;
    }
}