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

package gaffer.operation.handler;

import com.google.common.collect.Lists;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.GetOperation.SeedMatchingType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public abstract class AbstractGetElementsHandlerTest {
    // Identifier prefixes
    protected static final String SOURCE = "source";
    protected static final String DEST = "dest";
    protected static final String SOURCE_DIR = "sourceDir";
    protected static final String DEST_DIR = "destDir";

    // Identifiers
    protected static final String SOURCE_1_EDGE_SEED = SOURCE + 1;
    protected static final String DEST_1_EDGE_SEED = DEST + 1;

    protected static final String SOURCE_2_ENTITY_SEED = SOURCE + 2;
    protected static final String DEST_2 = DEST + 2;

    protected static final String SOURCE_3 = SOURCE + 3;
    protected static final String DEST_3_ENTITY_SEED = DEST + 3;

    protected static final String SOURCE_DIR_1_EDGE_SEED = SOURCE_DIR + 1;
    protected static final String DEST_DIR_1_EDGE_SEED = DEST_DIR + 1;

    protected static final String SOURCE_DIR_2_ENTITY_SEED = SOURCE_DIR + 2;
    protected static final String DEST_DIR_2 = DEST_DIR + 2;

    protected static final String SOURCE_DIR_3 = SOURCE_DIR + 3;
    protected static final String DEST_DIR_3_ENTITY_SEED = DEST_DIR + 3;

    // ElementSeed Seeds
    protected static final List<ElementSeed> ENTITY_SEEDS_EXIST = Arrays.asList(
            (ElementSeed) new EntitySeed(SOURCE_2_ENTITY_SEED),
            new EntitySeed(DEST_3_ENTITY_SEED),
            new EntitySeed(SOURCE_DIR_2_ENTITY_SEED),
            new EntitySeed(DEST_DIR_3_ENTITY_SEED));

    protected static final List<ElementSeed> EDGE_SEEDS_EXIST = Collections.singletonList(
            (ElementSeed) new EdgeSeed(SOURCE_1_EDGE_SEED, DEST_1_EDGE_SEED, false));

    protected static final List<ElementSeed> EDGE_DIR_SEEDS_EXIST = Collections.singletonList(
            (ElementSeed) new EdgeSeed(SOURCE_DIR_1_EDGE_SEED, DEST_DIR_1_EDGE_SEED, true));

    protected static final List<ElementSeed> EDGE_SEEDS_DONT_EXIST = Arrays.asList(
            (ElementSeed) new EdgeSeed(SOURCE_1_EDGE_SEED, "dest2DoesNotExist", false),
            new EdgeSeed("source2DoesNotExist", DEST_1_EDGE_SEED, false),
            new EdgeSeed(SOURCE_1_EDGE_SEED, DEST_1_EDGE_SEED, true));// does not exist

    protected static final List<ElementSeed> ENTITY_SEEDS_DONT_EXIST = Collections.singletonList(
            (ElementSeed) new EntitySeed("idDoesNotExist"));

    protected static final List<ElementSeed> ENTITY_SEEDS = getEntitySeeds();
    protected static final List<ElementSeed> EDGE_SEEDS = getEdgeSeeds();
    protected static final List<ElementSeed> ALL_SEEDS = getAllSeeds();
    protected static final List<Object> ALL_SEED_VERTICES = getAllSeededVertices();

    @Test
    public void shouldGetElements() throws Exception {
        for (boolean includeEntities : Arrays.asList(true, false)) {
            for (IncludeEdgeType includeEdgeType : IncludeEdgeType.values()) {
                if (!includeEntities && IncludeEdgeType.NONE == includeEdgeType) {
                    // Cannot query for nothing!
                    continue;
                }
                for (IncludeIncomingOutgoingType inOutType : IncludeIncomingOutgoingType.values()) {
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

    protected abstract Store createMockStore();

    protected abstract String getEdgeGroup();

    protected abstract String getEntityGroup();

    protected abstract void addEdges(Collection<Edge> edges, Store mockStore);

    protected abstract void addEntities(Collection<Entity> entities, Store mockStore);

    protected abstract OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> createHandler();

    protected abstract View createView();

    protected void shouldGetElementsBySeed(boolean includeEntities, final IncludeEdgeType includeEdgeType, final IncludeIncomingOutgoingType inOutType) throws Exception {
        final List<ElementSeed> expectedSeeds = new ArrayList<>();
        if (includeEntities) {
            expectedSeeds.addAll(ENTITY_SEEDS_EXIST);
        }
        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.DIRECTED == includeEdgeType) {
            expectedSeeds.addAll(EDGE_DIR_SEEDS_EXIST);
        }
        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.UNDIRECTED == includeEdgeType) {
            expectedSeeds.addAll(EDGE_SEEDS_EXIST);
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

        shouldGetElements(expectedSeeds, SeedMatchingType.EQUAL, includeEdgeType, includeEntities, inOutType, seeds);
    }

    protected void shouldGetRelatedElements(boolean includeEntities, final IncludeEdgeType includeEdgeType, final IncludeIncomingOutgoingType inOutType) throws Exception {
        final List<ElementSeed> expectedSeeds = new ArrayList<>();
        if (includeEntities) {
            for (Object identifier : ALL_SEED_VERTICES) {
                expectedSeeds.add(new EntitySeed(identifier));
            }
        }

        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.DIRECTED == includeEdgeType) {
            expectedSeeds.addAll(Collections.singletonList(new EdgeSeed(SOURCE_DIR_1_EDGE_SEED, DEST_DIR_1_EDGE_SEED, true)));

            if (IncludeIncomingOutgoingType.BOTH == inOutType || IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                expectedSeeds.addAll(Collections.singletonList((ElementSeed) new EdgeSeed(SOURCE_DIR_2_ENTITY_SEED, DEST_DIR_2, true)));
            }

            if (IncludeIncomingOutgoingType.BOTH == inOutType || IncludeIncomingOutgoingType.INCOMING == inOutType) {
                expectedSeeds.addAll(Collections.singletonList((ElementSeed) new EdgeSeed(SOURCE_DIR_3, DEST_DIR_3_ENTITY_SEED, true)));
            }
        }

        if (IncludeEdgeType.ALL == includeEdgeType || IncludeEdgeType.UNDIRECTED == includeEdgeType) {
            expectedSeeds.addAll(Arrays.asList(new EdgeSeed(SOURCE_1_EDGE_SEED, DEST_1_EDGE_SEED, false),
                    new EdgeSeed(SOURCE_2_ENTITY_SEED, DEST_2, false),
                    new EdgeSeed(SOURCE_3, DEST_3_ENTITY_SEED, false)));
        }

        shouldGetElements(expectedSeeds, SeedMatchingType.RELATED, includeEdgeType, includeEntities, inOutType, ALL_SEEDS);
    }

    protected void shouldGetElements(final List<ElementSeed> expectedSeeds, final SeedMatchingType seedMatching,
                                     final IncludeEdgeType includeEdgeType,
                                     final Boolean includeEntities,
                                     final IncludeIncomingOutgoingType inOutType,
                                     final Iterable<ElementSeed> seeds) throws IOException, OperationException {
        // Given
        final Store mockStore = createMockStore();
        final GetElements<ElementSeed, Element> operation = createMockOperation(seedMatching, includeEdgeType, includeEntities, inOutType, seeds);
        final OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> handler = createHandler();
        final Map<EdgeSeed, Edge> edges = getEdges();
        final Map<EntitySeed, Entity> entities = getEntities();
        addEntities(entities.values(), mockStore);
        addEdges(edges.values(), mockStore);

        // When
        final Iterable<? extends Element> results = handler.doOperation(operation, mockStore);

        // Then
        Map<? extends ElementSeed, ? extends Element> elements = new HashMap<>(edges);
        elements.putAll(((Map) entities));

        final List<ElementSeed> expectedSeedsCopy = Lists.newArrayList(expectedSeeds);
        for (Element result : results) {
            final ElementSeed seed = ElementSeed.createSeed(result);
            if (seed instanceof EntitySeed) {
                assertTrue("ElementSeed was not expected: " + seed, expectedSeeds.contains(seed));
            } else {
                EdgeSeed edgeSeed = ((EdgeSeed) seed);
                if (edgeSeed.isDirected()) {
                    assertTrue("ElementSeed was not expected: " + seed, expectedSeeds.contains(seed));
                } else {
                    final EdgeSeed idReversed = new EdgeSeed(edgeSeed.getDestination(), edgeSeed.getSource(), false);
                    expectedSeedsCopy.remove(idReversed);
                    assertTrue("ElementSeed was not expected: " + seed, expectedSeeds.contains(seed) || expectedSeeds.contains(idReversed));
                }
            }
            expectedSeedsCopy.remove(seed);
        }

        Assert.assertEquals("The number of elements returned was not as expected. Missing seeds: " + expectedSeedsCopy, expectedSeeds.size(),
                Lists.newArrayList(results).size());
    }

    protected Map<EdgeSeed, Edge> getEdges() {
        final Map<EdgeSeed, Edge> edges = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            addEdge(new Edge(getEdgeGroup(), SOURCE + i, DEST + i, false), edges);
            addEdge(new Edge(getEdgeGroup(), SOURCE_DIR + i, DEST_DIR + i, true), edges);
        }

        return edges;
    }

    protected Map<EntitySeed, Entity> getEntities() {
        final Map<EntitySeed, Entity> entities = new HashMap<>();
        for (int i = 0; i <= 10; i++) {
            addEntity(new Entity(getEntityGroup(), SOURCE + i), entities);
            addEntity(new Entity(getEntityGroup(), DEST + i), entities);
            addEntity(new Entity(getEntityGroup(), SOURCE_DIR + i), entities);
            addEntity(new Entity(getEntityGroup(), DEST_DIR + i), entities);
        }

        return entities;
    }

    protected GetElements<ElementSeed, Element> createMockOperation(final SeedMatchingType seedMatching,
                                                                    final GetOperation.IncludeEdgeType includeEdgeType,
                                                                    final Boolean includeEntities,
                                                                    final IncludeIncomingOutgoingType inOutType,
                                                                    final Iterable<ElementSeed> seeds) throws IOException {
        final GetElements<ElementSeed, Element> operation = mock(GetElements.class);
        given(operation.getSeeds()).willReturn(seeds);

        final Map<String, String> options = new HashMap<>();
        given(operation.getOptions()).willReturn(options);
        given(operation.getSeedMatching()).willReturn(seedMatching);
        given(operation.isPopulateProperties()).willReturn(true);
        given(operation.isIncludeEntities()).willReturn(includeEntities);
        given(operation.getIncludeEdges()).willReturn(includeEdgeType);
        given(operation.getIncludeIncomingOutGoing()).willReturn(inOutType);
        given(operation.validateFlags(Mockito.any(Edge.class))).willReturn(true);
        given(operation.validateFlags(Mockito.any(Entity.class))).willReturn(true);
        given(operation.validate(Mockito.any(Edge.class))).willReturn(true);
        given(operation.validate(Mockito.any(Entity.class))).willReturn(true);
        given(operation.validateFilter(Mockito.any(Element.class))).willReturn(true);

        final View view = createView();
        given(operation.getView()).willReturn(view);

        return operation;
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
        for (ElementSeed elementSeed : ENTITY_SEEDS_EXIST) {
            allSeededVertices.add(((EntitySeed) elementSeed).getVertex());
        }

        for (ElementSeed elementSeed : EDGE_SEEDS_EXIST) {
            allSeededVertices.add(((EdgeSeed) elementSeed).getSource());
            allSeededVertices.add(((EdgeSeed) elementSeed).getDestination());
        }

        for (ElementSeed elementSeed : EDGE_DIR_SEEDS_EXIST) {
            allSeededVertices.add(((EdgeSeed) elementSeed).getSource());
            allSeededVertices.add(((EdgeSeed) elementSeed).getDestination());
        }

        return allSeededVertices;
    }

    private void addEdge(final Edge element, final Map<EdgeSeed, Edge> edges) {
        edges.put(ElementSeed.createSeed(element), element);
    }

    private void addEntity(final Entity element, final Map<EntitySeed, Entity> entities) {
        entities.put(ElementSeed.createSeed(element), element);
    }

}