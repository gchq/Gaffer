/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
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
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.AGE_OFF_TIME;

public class GetWalksITTemplate extends AbstractStoreIT {
    final EntitySeed seedA = new EntitySeed("A");
    final EntitySeed seedE = new EntitySeed("E");


    @GafferTest
    public void shouldGetPaths(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithWhileRepeat(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(new While.Builder<>()
                        .operation(operation)
                        .maxRepeats(2)
                        .build())
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithWhile(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithPruning(final GafferTestCase testCase) throws Exception {
        // Given
        final StoreProperties properties = testCase.getStoreProperties().clone();
        properties.setOperationDeclarationPaths("getWalksWithPruningDeclaration.json");
        Graph graph = new Graph.Builder()
            .storeProperties(properties)
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED", getPaths(results));
    }

    @GafferTest
    public void shouldReturnNoResultsWhenNoEntityResults(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

        final GetWalks op = new Builder()
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
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals(0, Lists.newArrayList(results).size());
    }

    @GafferTest
    public void shouldGetPathsWithEntities(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(getElements, getElements, getEntities)
                .build();

        // When
        final List<Walk> results = Lists.newArrayList(graph.execute(op, new User()));

        // Then
        assertEquals("ABC,AED", getPaths(results));
        results.forEach(r -> r.getEntities().forEach(l -> {
            assertFalse(l.isEmpty());
        }));
    }

    @GafferTest
    public void shouldThrowExceptionIfGetPathsWithHopContainingNoEdges(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(getElements, getEntities, getElements)
                .build();

        // When / Then
        Exception exception = assertThrows(Exception.class, () -> graph.execute(op, new User()));
        assertTrue(exception.getMessage().contains("must contain a single hop"));
    }

    @GafferTest
    public void shouldGetPathsWithMultipleSeeds(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA, seedE)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED,EDA", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithMultipleEdgeTypes(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED,AEF", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithMultipleSeedsAndMultipleEdgeTypes(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA, seedE)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED,AEF,EDA,EFC", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithLoops(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("AEDA,AEFC", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithLoops_2(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("AEDAB,AEDAE", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithLoops_3(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE_3, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation, operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("AAAAA", getPaths(results));
    }

    @GafferTest
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void shouldGetPathsWithPreFiltering_1(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operationChain)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("AED", getPaths(results));
    }

    @GafferTest
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void shouldGetPartialPaths(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operationChain)
                .includePartial()
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("AB,AED", getPaths(results));
    }

    @GafferTest
    @TraitRequirement(StoreTrait.POST_AGGREGATION_FILTERING)
    public void shouldGetPathsWithPreFiltering_2(final GafferTestCase testCase) throws Exception {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operationChain, operationChain)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithModifiedViews(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(new GraphConfig("test"))
            .build();
        addDefaultElements(graph);

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

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC,AED", getPaths(results));
    }

    @GafferTest
    public void shouldGetPathsWithSimpleGraphHook_1(final GafferTestCase testCase) throws Exception {
        // Given
        final AddOperationsToChain graphHook = new AddOperationsToChain();
        graphHook.setEnd(Lists.newArrayList(new Limit.Builder<>().resultLimit(1).build()));

        final GraphConfig config = new GraphConfig.Builder().addHook(graphHook).graphId("integrationTest").build();

        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(config)
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals(1, Lists.newArrayList(results).size());
    }

    @GafferTest
    public void shouldGetPathsWithSimpleGraphHook_2(final GafferTestCase testCase) throws Exception {
        // Given
        final AddOperationsToChain graphHook = new AddOperationsToChain();
        final java.util.Map<String, List<Operation>> graphHookConfig = new HashMap<>();
        graphHookConfig.put(GetElements.class.getName(), Lists.newArrayList(new Limit.Builder<>().resultLimit(1).build()));
        graphHook.setAfter(graphHookConfig);

        final GraphConfig config = new GraphConfig.Builder().addHook(graphHook).graphId("integrationTest").build();

        Graph graph = new Graph.Builder()
            .storeProperties(testCase.getStoreProperties())
            .addSchema(createSchema())
            .config(config)
            .build();
        addDefaultElements(graph);

        final GetElements operation = new GetElements.Builder()
                .directedType(DirectedType.DIRECTED)
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build()).inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .build();

        final GetWalks op = new Builder()
                .input(seedA)
                .operations(operation, operation)
                .build();

        // When
        final Iterable<Walk> results = graph.execute(op, new User());

        // Then
        assertEquals("ABC", getPaths(results));
    }

    public static class AssertEntityIdsUnwrapped extends KorypheFunction<Object, Object> {
        @Override
        public Object apply(final Object obj) {
            // Check the vertices have been extracted correctly.
            assertTrue(obj instanceof Iterable);
            for (final Object item : (Iterable) obj) {
                assertFalse(item instanceof EntityId);
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

    public void addDefaultElements(final Graph graph) throws OperationException {
        graph.execute(new AddElements.Builder()
                .input(createEntitySet())
                .build(), new User());

        graph.execute(new AddElements.Builder()
                .input(createEdgeSet())
                .build(), new User());
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
        List<String> paths = new ArrayList<>();
        walks.forEach(e -> paths.add(e.getVerticesOrdered().stream().map(Object::toString).collect(Collectors.joining(""))));
        return paths.stream().sorted().collect(Collectors.joining(","));
    }
}
