/*
 * Copyright 2016-2019 Crown Copyright
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
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;

public class GetElementsIT extends AbstractStoreIT {
    // ElementId Seeds
    public static final Collection<ElementId> ENTITY_SEEDS_EXIST =
            Arrays.asList(
                    (ElementId) new EntitySeed(SOURCE_2),
                    new EntitySeed(DEST_3),
                    new EntitySeed(SOURCE_DIR_2),
                    new EntitySeed(DEST_DIR_3));

    public static final Collection<Element> ENTITIES_EXIST =
            getElements(ENTITY_SEEDS_EXIST, null);

    public static final Collection<ElementId> EDGE_SEEDS_EXIST =
            Arrays.asList((ElementId) new EdgeSeed(SOURCE_1, DEST_1, false),
                    (ElementId) new EdgeSeed(VERTEX_PREFIXES[0] + 0, VERTEX_PREFIXES[1] + 0),
                    (ElementId) new EdgeSeed(VERTEX_PREFIXES[0] + 2, VERTEX_PREFIXES[1] + 2));

    public static final Collection<ElementId> EDGE_SEEDS_BOTH =
            Arrays.asList((ElementId) new EdgeSeed(VERTEX_PREFIXES[0] + 0, VERTEX_PREFIXES[1] + 0),
                    (ElementId) new EdgeSeed(VERTEX_PREFIXES[0] + 2, VERTEX_PREFIXES[1] + 2));

    public static final Collection<Element> EDGES_EXIST =
            getElements(EDGE_SEEDS_EXIST, false);

    public static final Collection<ElementId> EDGE_DIR_SEEDS_EXIST =
            Arrays.asList((ElementId) new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true),
                    (ElementId) new EdgeSeed(VERTEX_PREFIXES[0] + 0, VERTEX_PREFIXES[1] + 0),
                    (ElementId) new EdgeSeed(VERTEX_PREFIXES[0] + 2, VERTEX_PREFIXES[1] + 2));

    public static final Collection<Element> EDGES_DIR_EXIST =
            getElements(EDGE_DIR_SEEDS_EXIST, true);

    public static final Collection<ElementId> EDGE_SEEDS_DONT_EXIST =
            Arrays.asList(
                    (ElementId) new EdgeSeed(SOURCE_1, "dest2DoesNotExist", false),
                    new EdgeSeed("source2DoesNotExist", DEST_1, false),
                    new EdgeSeed(SOURCE_1, DEST_1, true)); // does not exist

    public static final Collection<ElementId> ENTITY_SEEDS_DONT_EXIST =
            Collections.singletonList(
                    (ElementId) new EntitySeed("idDoesNotExist"));

    public static final Collection<ElementId> ENTITY_SEEDS = getEntityIds();
    public static final Collection<ElementId> EDGE_SEEDS = getEdgeIds();
    public static final Collection<ElementId> ALL_SEEDS = getAllSeeds();
    public static final Collection<Object> ALL_SEED_VERTICES = getAllSeededVertices();

    @Override
    public void _setup() throws Exception {
        addDefaultElements();
    }

    @Test
    public void shouldGetElements() {
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
                        } catch (final Throwable e) {
                            throw new AssertionError("GetElementsBySeed failed with parameters: \nincludeEntities=" + includeEntities
                                    + " \nincludeEdges=" + includeEdges + " \ndirectedType=" + directedType + " \ninOutType=" + inOutType, e);
                        }

                        try {
                            shouldGetRelatedElements(includeEntities, includeEdges, directedType, inOutType);
                        } catch (final Throwable e) {
                            throw new AssertionError("GetRelatedElements failed with parameters: \nincludeEntities=" + includeEntities
                                    + " \nincludeEdges=" + includeEdges + " \ndirectedType=" + directedType + " \ninOutType=" + inOutType, e);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldGetAllEdgesWhenFlagSet() throws Exception {
        // Given
        final User user = new User();

        final GetElements opExcludingAllEdges = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_1), new EntitySeed(DEST_2))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final GetElements opIncludingAllEdges = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_1), new EntitySeed(DEST_2))
                .view(new View.Builder()
                        .allEdges(true)
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> resultsExcludingAllEdges = graph.execute(opExcludingAllEdges, user);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(SOURCE_1)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(DEST_2)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                resultsExcludingAllEdges);

        // When
        final CloseableIterable<? extends Element> resultsIncludingAllEdges = graph.execute(opIncludingAllEdges, user);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_1)
                        .dest(DEST_1)
                        .directed(false)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_2)
                        .dest(DEST_2)
                        .directed(false)
                        .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                resultsIncludingAllEdges);
    }

    @Test
    public void shouldGetAllEntitiesWhenFlagSet() throws OperationException {
        // Given
        final User user = new User();

        final GetElements opExcludingAllEntities = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_1), new EntitySeed(DEST_2))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        final GetElements opIncludingAllEntities = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_1), new EntitySeed(DEST_2))
                .view(new View.Builder()
                        .allEntities(true)
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> resultsExcludingAllEntities = graph.execute(opExcludingAllEntities, user);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_1)
                        .dest(DEST_1)
                        .directed(false)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_2)
                        .dest(DEST_2)
                        .directed(false)
                        .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                resultsExcludingAllEntities);

        // When
        final CloseableIterable<? extends Element> resultsIncludingAllEntities = graph.execute(opIncludingAllEntities, user);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(SOURCE_1)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(DEST_2)
                        .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                resultsIncludingAllEntities);

    }

    @TraitRequirement(StoreTrait.MATCHED_VERTEX)
    @Test
    public void shouldGetElementsWithMatchedVertex() throws Exception {
        // Given
        final User user = new User();

        final GetElements op = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_DIR_1), new EntitySeed(DEST_DIR_2), new EntitySeed(SOURCE_DIR_3))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_DIR_1)
                        .dest(DEST_DIR_1)
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_DIR_2)
                        .dest(DEST_DIR_2)
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_DIR_3)
                        .dest(DEST_DIR_3)
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                results);
    }

    @Test
    @TraitRequirement(StoreTrait.MATCHED_VERTEX)
    public void shouldGetElementsWithMatchedVertexFilter() throws Exception {
        // Given
        final User user = new User();

        final GetElements op = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_DIR_1), new EntitySeed(DEST_DIR_2), new EntitySeed(SOURCE_DIR_3))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.ADJACENT_MATCHED_VERTEX.name())
                                        .execute(new IsIn(DEST_DIR_1, DEST_DIR_2, DEST_DIR_3))
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_DIR_1)
                        .dest(DEST_DIR_1)
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(SOURCE_DIR_3)
                        .dest(DEST_DIR_3)
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property(TestPropertyNames.INT, 1)
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
                ),
                results);
    }

    @Test
    public void shouldGetElementsWithProvidedProperties() throws Exception {
        // Given
        final User user = new User();

        final GetElements op = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_2))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        for (final Element result : results) {
            assertEquals(1, result.getProperties().size());
            assertEquals(1L, result.getProperties().get(TestPropertyNames.COUNT));
        }
    }

    @Test
    public void shouldGetElementsWithExcludedProperties() throws Exception {
        // Given
        final User user = new User();

        final GetElements op = new GetElements.Builder()
                .input(new EntitySeed(SOURCE_2))
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        for (final Element result : results) {
            assertEquals(1, result.getProperties().size());
            assertEquals(1L, result.getProperties().get(TestPropertyNames.COUNT));
        }
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetElementsBySeed() throws Exception {
        // Given
        final GetElements op = new GetElements.Builder()
                .input(new EmptyClosableIterable<>())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertFalse(results.iterator().hasNext());
    }

    @Test
    public void shouldReturnEmptyIteratorIfNoSeedsProvidedForGetRelatedElements() throws Exception {
        // Given
        final GetElements op = new GetElements.Builder()
                .input(new EmptyClosableIterable<>())
                .build();
        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertFalse(results.iterator().hasNext());
    }

    private void shouldGetElementsBySeed(final boolean includeEntities,
                                         final boolean includeEdges,
                                         final DirectedType directedType,
                                         final IncludeIncomingOutgoingType inOutType) throws Exception {
        final Set<Element> expectedElements = new HashSet<>();
        if (includeEntities) {
            expectedElements.addAll(ENTITIES_EXIST);
        }

        if (includeEdges) {
            if (DirectedType.isDirected(directedType)) {
                expectedElements.addAll(EDGES_DIR_EXIST);
            }
            if (DirectedType.isUndirected(directedType)) {
                expectedElements.addAll(EDGES_EXIST);
            }
        }

        final Collection<ElementId> seeds;
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
        final Set<ElementId> expectedElementIds = new HashSet<>();
        final Set<Element> expectedElements = new HashSet<>();
        if (includeEntities) {
            for (final Object identifier : ALL_SEED_VERTICES) {
                final EntityId entityId = new EntitySeed(identifier);
                expectedElementIds.add(entityId);
            }
        }

        if (includeEdges) {
            expectedElementIds.addAll(EDGE_SEEDS_BOTH);

            if (DirectedType.UNDIRECTED != directedType) {
                expectedElementIds.add(new EdgeSeed(SOURCE_DIR_1, DEST_DIR_1, true));

                if (null == inOutType || IncludeIncomingOutgoingType.EITHER == inOutType || IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                    expectedElementIds.add(new EdgeSeed(SOURCE_DIR_2, DEST_DIR_2, true));
                }

                if (null == inOutType || IncludeIncomingOutgoingType.EITHER == inOutType || IncludeIncomingOutgoingType.INCOMING == inOutType) {
                    expectedElementIds.add(new EdgeSeed(SOURCE_DIR_3, DEST_DIR_3, true, EdgeId.MatchedVertex.DESTINATION));
                }
            }

            if (DirectedType.DIRECTED != directedType) {
                expectedElementIds.add(new EdgeSeed(SOURCE_1, DEST_1, false));
                expectedElementIds.add(new EdgeSeed(SOURCE_2, DEST_2, false));
                expectedElementIds.add(new EdgeSeed(SOURCE_3, DEST_3, false, EdgeId.MatchedVertex.DESTINATION));
            }
        }

        expectedElements.addAll(getElements(expectedElementIds, null));
        if (DirectedType.DIRECTED == directedType) {
            expectedElements.removeIf(e -> e instanceof Edge && ((Edge) e).isUndirected());
        }
        if (DirectedType.UNDIRECTED == directedType) {
            expectedElements.removeIf(e -> e instanceof Edge && ((Edge) e).isDirected());
        }
        shouldGetElements(expectedElements, SeedMatchingType.RELATED, directedType, includeEntities, includeEdges, inOutType, ALL_SEEDS);
    }

    private void shouldGetElements(final Collection<Element> expectedElements,
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
                .seedMatching(seedMatching)
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, user);

        // Then
        ElementUtil.assertElementEquals(expectedElements, results, true);
    }

    private static Collection<Element> getElements(final Collection<ElementId> seeds, final Boolean direction) {
        final Set<Element> elements = new HashSet<>(seeds.size());
        for (final ElementId seed : seeds) {
            if (seed instanceof EntityId) {
                final Entity entity = new Entity(TestGroups.ENTITY, ((EntityId) seed).getVertex());
                entity.putProperty(TestPropertyNames.COUNT, 1L);
                entity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
                elements.add(entity);
            } else {
                if (DirectedType.isEither(((EdgeId) seed).getDirectedType())) {
                    if (BooleanUtils.isNotTrue(direction)) {
                        final Edge edge = new Edge.Builder()
                                .group(TestGroups.EDGE)
                                .source(((EdgeId) seed).getSource())
                                .dest(((EdgeId) seed).getDestination())
                                .matchedVertex(((EdgeId) seed).getMatchedVertex())
                                .directed(false)
                                .property(TestPropertyNames.INT, 1)
                                .property(TestPropertyNames.COUNT, 1L)
                                .build();
                        elements.add(edge);
                    }
                    if (BooleanUtils.isNotFalse(direction)) {
                        final Edge edgeDir = new Edge.Builder()
                                .group(TestGroups.EDGE)
                                .source(((EdgeId) seed).getSource())
                                .dest(((EdgeId) seed).getDestination())
                                .matchedVertex(((EdgeId) seed).getMatchedVertex())
                                .directed(true)
                                .property(TestPropertyNames.INT, 1)
                                .property(TestPropertyNames.COUNT, 1L)
                                .build();
                        elements.add(edgeDir);
                    }
                } else {
                    final Edge edge = new Edge.Builder()
                            .group(TestGroups.EDGE)
                            .source(((EdgeId) seed).getSource())
                            .dest(((EdgeId) seed).getDestination())
                            .directed(((EdgeId) seed).isDirected())
                            .matchedVertex(((EdgeId) seed).getMatchedVertex())
                            .property(TestPropertyNames.INT, 1)
                            .property(TestPropertyNames.COUNT, 1L)
                            .build();
                    elements.add(edge);
                }
            }
        }

        return elements;
    }

    private static Collection<ElementId> getEntityIds() {
        Set<ElementId> allSeeds = new HashSet<>();
        allSeeds.addAll(ENTITY_SEEDS_EXIST);
        allSeeds.addAll(ENTITY_SEEDS_DONT_EXIST);
        return allSeeds;
    }

    private static Collection<ElementId> getEdgeIds() {
        Set<ElementId> allSeeds = new HashSet<>();
        allSeeds.addAll(EDGE_SEEDS_EXIST);
        allSeeds.addAll(EDGE_DIR_SEEDS_EXIST);
        allSeeds.addAll(EDGE_SEEDS_DONT_EXIST);
        allSeeds.addAll(EDGE_SEEDS_BOTH);
        return allSeeds;
    }

    private static Collection<ElementId> getAllSeeds() {
        Set<ElementId> allSeeds = new HashSet<>();
        allSeeds.addAll(ENTITY_SEEDS);
        allSeeds.addAll(EDGE_SEEDS);
        return allSeeds;
    }

    private static Collection<Object> getAllSeededVertices() {
        Set<Object> allSeededVertices = new HashSet<>();
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
