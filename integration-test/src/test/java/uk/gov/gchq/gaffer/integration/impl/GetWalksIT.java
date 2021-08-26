/*
 * Copyright 2017-2021 Crown Copyright
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
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ExtractProperty;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.data.graph.function.walk.ExtractWalkEntities;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.GetWalks.Builder;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.function.IterableConcat;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.CollectionContains;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class GetWalksIT extends AbstractStoreIT {
    final EntitySeed seedA = new EntitySeed("A");
    final EntitySeed seedE = new EntitySeed("E");

    @Override
    public void _setup() throws Exception {
        addDefaultElements();
    }

    @Test
    public void shouldGetPaths() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldGetPathsWithWhileRepeat() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(new While.Builder<>()
                        .operation(operation)
                        .maxRepeats(2)
                        .build())
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldGetPathsWithWhile() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(new While.Builder<>()
                        .conditional(
                                new Conditional(
                                        new Exists(), // This will always be true
                                        new Map.Builder<>()
                                                .first(new AssertEntityIdsUnwrapped())
                                                .build()
                                )
                        )
                        .operation(operation)
                        .maxRepeats(2)
                        .build())
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldGetPathsWithPruning() throws Exception {
        // Given
        final StoreProperties properties = getStoreProperties();
        properties.setOperationDeclarationPaths("getWalksWithPruningDeclaration.json");
        createGraph(properties);
        addDefaultElements();

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldReturnNoResultsWhenNoEntityResults() throws Exception {
        // Given
        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(
                        new GetElements.Builder()
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build(),
                        new OperationChain.Builder()
                                .first(new GetElements.Builder()
                                        .view(new View.Builder()
                                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                                        .preAggregationFilter(new ElementFilter.Builder()
                                                                .select(TestPropertyNames.INT)
                                                                .execute(new IsMoreThan(10000))
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .then(new GetElements())
                                .build())
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(Lists.newArrayList(results)).isEmpty();
    }

    @Test
    public void shouldGetPathsWithEntities() throws Exception {
        // Given
        final GetElements getEntities = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final GetElements getElements = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(getElements, getElements, getEntities)
                .build();

        // When
        final List<Walk> results = Lists.newArrayList(graph.execute(op, getUser()));

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC");
        results.forEach(r -> r.getEntities().forEach(l -> {
            assertThat(l).isNotEmpty();
        }));
    }

    @Test
    public void shouldThrowExceptionIfGetPathsWithHopContainingNoEdges() throws Exception {
        // Given
        final GetElements getEntities = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final GetElements getElements = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(getElements, getEntities, getElements)
                .build();

        // When / Then
        try {
            Lists.newArrayList(graph.execute(op, getUser()));
        } catch (final Exception e) {
            assertThat(e.getMessage()).as(e.getMessage()).contains("must contain a single hop");
        }
    }

    @Test
    public void shouldGetPathsWithMultipleSeeds() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA, seedE)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC,EDA");
    }

    @Test
    public void shouldGetPathsWithMultipleEdgeTypes() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,AEF,ABC");
    }

    @Test
    public void shouldGetPathsWithMultipleSeedsAndMultipleEdgeTypes() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA, seedE)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,AEF,ABC,EDA,EFC");
    }

    @Test
    public void shouldGetPathsWithLoops() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AEDA,AEFC");
    }

    @Test
    public void shouldGetPathsWithLoops_2() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AEDAE,AEDAB");
    }

    @Test
    public void shouldGetPathsWithLoops_3() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE_3, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AAAAA");
    }

    @Test
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void shouldGetPathsWithPreFiltering_1() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final OperationChain operationChain = new OperationChain.Builder()
                // only walk down entities which have a property set to an integer
                //larger than 3.
                .first(new GetElements.Builder()
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.PROP_1)
                                                .execute(new IsMoreThan(3))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(operation)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operationChain)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED");
    }

    @Test
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void shouldGetPartialPaths() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final OperationChain operationChain = new OperationChain.Builder()
                // only walk down entities which have a property set to an integer
                //larger than 3.
                .first(new GetElements.Builder()
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.PROP_1)
                                                .execute(new IsMoreThan(3))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(operation)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operationChain)
                .includePartial()
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,AB");
    }

    @Test
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void shouldGetPathsWithPreFiltering_2() throws Exception {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final OperationChain operationChain = new OperationChain.Builder()
                // only walk down entities which have a property set to an integer
                // less than 3.
                .first(new GetElements.Builder()
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.PROP_1)
                                                .execute(new IsLessThan(3))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(operation)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operationChain, operationChain)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("ABC");
    }

    @Test
    public void shouldGetPathsWithModifiedViews() throws OperationException {
        // Given
        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(TestPropertyNames.COUNT)
                                        .execute(new IsMoreThan(0L))
                                        .build())
                                .build())
                        .build())
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldGetPathsWithSimpleGraphHook_1() throws Exception {
        // Given
        final AddOperationsToChain graphHook = new AddOperationsToChain();
        graphHook.setEnd(Lists.newArrayList(new Limit.Builder<>().resultLimit(1).build()));

        final GraphConfig config = new GraphConfig.Builder().addHook(graphHook).graphId("integrationTest").build();
        createGraph(config);

        addDefaultElements();

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(Lists.newArrayList(results)).hasSize(1);
    }

    @Test
    public void shouldGetPathsWithSimpleGraphHook_2() throws Exception {
        // Given
        final AddOperationsToChain graphHook = new AddOperationsToChain();
        final java.util.Map<String, List<Operation>> graphHookConfig = new HashMap<>();
        graphHookConfig.put(GetElements.class.getName(), Lists.newArrayList(new Limit.Builder<>().resultLimit(1).build()));
        graphHook.setAfter(graphHookConfig);

        final GraphConfig config = new GraphConfig.Builder().addHook(graphHook).graphId("integrationTest").build();
        createGraph(config);

        addDefaultElements();

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new GetWalks.Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, getUser());

        // Then
        assertThat(getPaths(results)).isEqualTo("ABC");
    }

    public static class AssertEntityIdsUnwrapped extends KorypheFunction<Object, Object> {
        @Override
        public Object apply(final Object obj) {
            // Check the vertices have been extracted correctly.
            assertThat(obj).isInstanceOf(Iterable.class);
            for (final Object item : (Iterable) obj) {
                assertThat(item).isNotInstanceOf(EntityId.class);
            }
            return obj;
        }
    }

    private Set<Entity> createEntitySet() {
        final Set<Entity> entities = new HashSet<>();

        final Entity firstEntity = new Entity(TestGroups.ENTITY, "A");
        firstEntity.putProperty(TestPropertyNames.STRING, "3");
        firstEntity.putProperty(TestPropertyNames.PROP_1, 1);
        entities.add(firstEntity);

        final Entity secondEntity = new Entity(TestGroups.ENTITY, "B");
        secondEntity.putProperty(TestPropertyNames.STRING, "3");
        secondEntity.putProperty(TestPropertyNames.PROP_1, 2);
        entities.add(secondEntity);

        final Entity thirdEntity = new Entity(TestGroups.ENTITY, "C");
        thirdEntity.putProperty(TestPropertyNames.STRING, "3");
        thirdEntity.putProperty(TestPropertyNames.PROP_1, 3);
        entities.add(thirdEntity);

        final Entity fourthEntity = new Entity(TestGroups.ENTITY, "D");
        fourthEntity.putProperty(TestPropertyNames.STRING, "3");
        fourthEntity.putProperty(TestPropertyNames.PROP_1, 4);
        entities.add(fourthEntity);

        final Entity fifthEntity = new Entity(TestGroups.ENTITY, "E");
        fifthEntity.putProperty(TestPropertyNames.STRING, "3");
        fifthEntity.putProperty(TestPropertyNames.PROP_1, 5);
        entities.add(fifthEntity);

        final Entity sixthEntity = new Entity(TestGroups.ENTITY, "F");
        sixthEntity.putProperty(TestPropertyNames.STRING, "3");
        sixthEntity.putProperty(TestPropertyNames.PROP_1, 6);
        entities.add(sixthEntity);

        return entities;
    }

    private Set<Edge> createEdgeSet() {
        final Set<Edge> edges = new HashSet<>();

        final Edge firstEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A")
                .dest("B")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(firstEdge);

        final Edge secondEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("B")
                .dest("C")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(secondEdge);

        final Edge thirdEdge = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("B")
                .dest("C")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(thirdEdge);

        final Edge fourthEdge = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("F")
                .dest("C")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(fourthEdge);

        final Edge fifthEdge = new Edge.Builder()
                .group(TestGroups.EDGE_2)
                .source("E")
                .dest("F")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(fifthEdge);

        final Edge sixthEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("E")
                .dest("D")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(sixthEdge);

        final Edge seventhEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("D")
                .dest("A")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(seventhEdge);

        final Edge eighthEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A")
                .dest("E")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(eighthEdge);

        final Edge ninthEdge = new Edge.Builder()
                .group(TestGroups.EDGE_3)
                .source("A")
                .dest("A")
                .directed(true)
                .property(TestPropertyNames.INT, 1)
                .property(TestPropertyNames.COUNT, 1L)
                .build();
        edges.add(ninthEdge);

        return edges;
    }

    @Override
    public void addDefaultElements() throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(createEntitySet())
                .build(), getUser());

        graph.execute(new AddElements.Builder()
                .input(createEdgeSet())
                .build(), getUser());
    }

    protected Schema createSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_EITHER, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .build())
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .type(TestTypes.TIMESTAMP_2, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new AgeOff(AGE_OFF_TIME))
                        .build())
                .type(TestTypes.PROP_INTEGER_2, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Max())
                        .validateFunctions(new IsLessThan(10))
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.STRING, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .edge(TestGroups.EDGE_3, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_EITHER)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .groupBy(TestPropertyNames.INT)
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP_2)
                        .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER_2)
                        .build())
                .build();
    }

    private String getPaths(final Iterable<Walk> walks) {
        final StringBuilder sb = new StringBuilder();
        for (final Walk walk : walks) {
            sb.append(walk.getVerticesOrdered().stream().map(Object::toString).collect(Collectors.joining("")));
            sb.append(',');
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    @Test
    public void shouldReturnAllWalksWhenConditionalIsNull() throws Exception {
        final Iterable<Walk> walks = executeGetWalksApplyingConditional(null);
        assertThat(getPaths(walks)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldReturnAllWalksWhenConditionalIsUnconfigured() throws Exception {
        final Iterable<Walk> walks = executeGetWalksApplyingConditional(new Conditional());
        assertThat(getPaths(walks)).isEqualTo("AED,ABC");
    }

    @Test
    public void shouldFilterWalksThatDoNotContainProperty5() throws Exception {
        final Iterable<Walk> walks = getWalksThatPassPredicateTest(new CollectionContains(5));
        assertThat(getPaths(walks)).isEqualTo("AED");
    }

    @Test
    public void shouldFilterWalksThatDoNotContainProperty2() throws Exception {
        final Iterable<Walk> walks = getWalksThatPassPredicateTest(new CollectionContains(2));
        assertThat(getPaths(walks)).isEqualTo("ABC");
    }

    @Test
    public void shouldFilterAllWalksWhenNoneContainProperty() throws Exception {
        final Iterable<Walk> walks = getWalksThatPassPredicateTest(new CollectionContains(6));
        assertThat(getPaths(walks)).isEmpty();
    }

    @Test
    public void shouldNotFilterAnyWalksWhenAllContainProperty() throws Exception {
        final Iterable<Walk> walks = getWalksThatPassPredicateTest(new CollectionContains(1));
        assertThat(getPaths(walks)).isEqualTo("AED,ABC");
    }

    private Iterable<Walk> getWalksThatPassPredicateTest(final Predicate predicate) throws Exception {
        final Conditional conditional = new Conditional();
        conditional.setTransform(new OperationChain.Builder()
                .first(new Map.Builder<>()
                        .first(new ExtractWalkEntities())
                        .then(new IterableConcat())
                        .build())
                .then(new ForEach.Builder<>()
                        .operation(new Map.Builder<>()
                                .first(new ExtractProperty(TestPropertyNames.PROP_1))
                                .build())
                        .build())
                .build());
        conditional.setPredicate(predicate);

        return executeGetWalksApplyingConditional(conditional);
    }

    @Test
    public void shouldFilterWalksUsingWalkPredicateWithoutTransform() throws Exception {
        final Conditional conditional = new Conditional();
        conditional.setPredicate(new WalkPredicate());
        final Iterable<Walk> walks = executeGetWalksApplyingConditional(conditional);
        assertThat(getPaths(walks)).isEqualTo("AED");
    }

    public static class WalkPredicate implements Predicate<Walk> {
        @Override
        public boolean test(final Walk walk) {
            return walk.getEntities().stream()
                    .flatMap(l -> l.stream())
                    .anyMatch(e -> e.getVertex().equals("E"));
        }
    }

    @Test
    public void shouldNotFilterWalksWhenNoPredicateSupplied() throws Exception {
        final Conditional conditional = new Conditional();
        conditional.setTransform(new OperationChain.Builder()
                .first(new Map.Builder<>()
                        .first(new ExtractWalkEntities())
                        .then(new IterableConcat())
                        .build())
                .then(new ForEach.Builder<>()
                        .operation(new Map.Builder<>()
                                .first(new ExtractProperty(TestPropertyNames.PROP_1))
                                .build())
                        .build())
                .build());

        final Iterable<Walk> walks = executeGetWalksApplyingConditional(conditional);
        assertThat(getPaths(walks)).isEqualTo("AED,ABC");
    }

    private Iterable<Walk> executeGetWalksApplyingConditional(final Conditional conditional) throws OperationException {
        final GetWalks op = new GetWalks.Builder()
                .operations(
                        new GetElements.Builder()
                                .directedType(DirectedType.DIRECTED)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                                .properties(TestPropertyNames.COUNT)
                                                .build())
                                        .entity(TestGroups.ENTITY)
                                        .build())
                                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                                .build(),
                        new GetElements.Builder()
                                .directedType(DirectedType.DIRECTED)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                                .properties(TestPropertyNames.COUNT)
                                                .build())
                                        .entity(TestGroups.ENTITY)
                                        .build())
                                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                                .build(),
                        new GetElements.Builder()
                                .view(new View.Builder()
                                        .entities(Lists.newArrayList(TestGroups.ENTITY))
                                        .build())
                                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                .conditional(conditional)
                .input(seedA)
                .build();

        return graph.execute(op, getUser());
    }
}
