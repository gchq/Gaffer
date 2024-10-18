/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSetsPairs;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetAllGraphInfoHandler;
import uk.gov.gchq.gaffer.federated.simple.util.FederatedTestUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetGraphCreatedTime;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static uk.gov.gchq.gaffer.federated.simple.util.FederatedTestUtils.StoreType;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class FederatedStoreIT {

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldFederateElementsByAggregation() throws StoreException, OperationException {
        // Given
        FederatedStore store = new FederatedStore();

        final String graphId1 = "graph1";
        final String graphId2 = "graph2";

        final Graph graph1 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.ACCUMULO);
        final Graph graph2 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2, StoreType.ACCUMULO);

        final String group = "person";
        final String vertex = "1";

        // Add the same vertex to different graphs but with different properties
        Properties graph1ElementProps = new Properties();
        graph1ElementProps.put("name", "marko");
        Entity graph1Entity = new Entity(group, vertex, graph1ElementProps);

        Properties graph2ElementProps = new Properties();
        graph2ElementProps.put("age", 29);
        Entity graph2Entity = new Entity(group, vertex, graph2ElementProps);

        // We expect only one entity to be returned that has merged properties
        Properties mergedProperties = new Properties();
        mergedProperties.putAll(graph1ElementProps);
        mergedProperties.putAll(graph2ElementProps);
        Entity expectedEntity = new Entity(group, vertex, mergedProperties);

        // Init store and add graphs
        store.initialise("federated", null, new StoreProperties());
        store.execute(
            new AddGraph.Builder()
                .graphConfig(graph1.getConfig())
                .schema(graph1.getSchema())
                .properties(graph1.getStoreProperties().getProperties()).build(),
            new Context());
        store.execute(
            new AddGraph.Builder()
                .graphConfig(graph2.getConfig())
                .schema(graph2.getSchema())
                .properties(graph2.getStoreProperties().getProperties()).build(),
            new Context());

        // Add element operations
        AddElements addGraph1Elements = new AddElements.Builder()
            .input(graph1Entity)
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1)
            .build();
        AddElements addGraph2Elements = new AddElements.Builder()
            .input(graph2Entity)
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId2)
            .build();

        // Add data into graphs (use the handle method to widen methods that are tested)
        store.handleOperation(addGraph1Elements, new Context());
        store.handleOperation(addGraph2Elements, new Context());

        // Run a get all on both graphs specifying that we want to merge elements
        OperationChain<Iterable<? extends Element>> getAllElements = new OperationChain.Builder()
            .first(new GetAllElements.Builder()
                    .build())
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1 + "," + graphId2)
            .option(FederatedOperationHandler.OPT_AGGREGATE_ELEMENTS, "true")
            .build();

        Iterable<? extends Element> result = store.execute(getAllElements, new Context());

        // Then
        assertThat(result).extracting(e -> (Element) e).containsOnly(expectedEntity);
    }

    @Test
    void shouldSkipGraphOnFailureIfSet() throws StoreException, OperationException {
        // Given
        String graphId = "graph";
        FederatedStore federatedStore = new FederatedStore();
        Context context = new Context();

        federatedStore.initialise("federated", null, new StoreProperties());
        federatedStore.setDefaultGraphIds(Arrays.asList(graphId));

        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new MapStoreProperties().getProperties())
                .build();

        // Add a graph
        federatedStore.execute(addGraph, new Context());

        // Run an operation that doesn't exist to the store check the option lets it pass
        final GetElementsBetweenSetsPairs getOperationNoOps = new GetElementsBetweenSetsPairs();
        final GetElementsBetweenSetsPairs getOperationWithOps = new GetElementsBetweenSetsPairs.Builder()
            .option(FederatedOperationHandler.OPT_SKIP_FAILED_EXECUTE, "true")
            .build();

        // Then
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> federatedStore.execute(getOperationNoOps, context));
        assertThatNoException()
            .isThrownBy(() -> federatedStore.execute(getOperationWithOps, context));
    }

    @Test
    void shouldAllowReturningSeparateResults() throws StoreException, OperationException {
        // Given
        FederatedStore store = new FederatedStore();

        final String graphId1 = "graph1";
        final String graphId2 = "graph2";

        final Graph graph1 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1,
                StoreType.MAP);
        final Graph graph2 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2,
                StoreType.MAP);

        final String group = "person";
        final String vertex = "1";

        // Add the same vertex to different graphs
        Properties graph1ElementProps = new Properties();
        graph1ElementProps.put("name", "marko");
        Entity graph1Entity = new Entity(group, vertex, graph1ElementProps);

        Properties graph2ElementProps = new Properties();
        graph2ElementProps.put("age", 29);
        Entity graph2Entity = new Entity(group, vertex, graph2ElementProps);

        // Init store and add graphs
        store.initialise("federated", null, new StoreProperties());
        store.execute(
                new AddGraph.Builder()
                        .graphConfig(graph1.getConfig())
                        .schema(graph1.getSchema())
                        .properties(graph1.getStoreProperties().getProperties()).build(),
                new Context());
        store.execute(
                new AddGraph.Builder()
                        .graphConfig(graph2.getConfig())
                        .schema(graph2.getSchema())
                        .properties(graph2.getStoreProperties().getProperties()).build(),
                new Context());

        // Add element operations
        AddElements addGraph1Elements = new AddElements.Builder()
                .input(graph1Entity)
                .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1)
                .build();
        AddElements addGraph2Elements = new AddElements.Builder()
                .input(graph2Entity)
                .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId2)
                .build();

        // Add data into graphs
        store.handleOperation(addGraph1Elements, new Context());
        store.handleOperation(addGraph2Elements, new Context());

        // Run a get all on both graphs specifying that we want separate results
        OperationChain<Iterable<? extends Element>> getAllElements = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                        .build())
                .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1 + "," + graphId2)
                .option(FederatedOperationHandler.OPT_SEPARATE_RESULTS, "true")
                .build();

        // Expect the results to be in a map instead
        Map<String, Iterable<? extends Element>> result = (Map<String, Iterable<? extends Element>>) store.execute(getAllElements, new Context());

        // Then
        assertThat(result).containsKeys(graphId1, graphId2);
        assertThat(result.get(graphId1)).extracting(e -> (Element) e).containsOnly(graph1Entity);
        assertThat(result.get(graphId2)).extracting(e -> (Element) e).containsOnly(graph2Entity);
    }

    @Test
    void shouldAddAndGetAllGraphs() throws StoreException, OperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId = "newGraph";

        // AddGraph operation
        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId))
                .schema(new Schema())
                .properties(new java.util.Properties())
                .build();

        // GetAllGraphIds operation
        final GetAllGraphIds getAllGraphIds = new GetAllGraphIds();

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        federatedStore.execute(addGraph, new Context());
        final Set<String> graphIds = federatedStore.execute(getAllGraphIds, new Context());

        assertThat(graphIds).containsExactly(graphId);
    }

    @Test
    void shouldExecuteOnAllGraphsExceptExcludedOnes() throws StoreException, OperationException {
        final String federatedGraphId = "federated";
        final String graphId1 = "newGraph1";
        final String graphId2 = "newGraph2";
        final String graphId3 = "newGraph3";

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(federatedGraphId, null, new StoreProperties());

        // AddGraph operations
        final AddGraph addGraph1 = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId1))
                .schema(new Schema())
                .properties(new MapStoreProperties().getProperties())
                .build();
        final AddGraph addGraph2 = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId2))
                .schema(new Schema())
                .properties(new MapStoreProperties().getProperties())
                .build();
        final AddGraph addGraph3 = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId3))
                .schema(new Schema())
                .properties(new MapStoreProperties().getProperties())
                .build();

        federatedStore.execute(addGraph1, new Context());
        federatedStore.execute(addGraph2, new Context());
        federatedStore.execute(addGraph3, new Context());

        final GetGraphCreatedTime operation = new GetGraphCreatedTime.Builder()
            .option(FederatedOperationHandler.OPT_EXCLUDE_GRAPH_IDS, graphId2)
            .build();

        Map<String, String> result = federatedStore.execute(operation, new Context());
        assertThat(result).containsOnlyKeys(graphId1, graphId3);
    }

    @Test
    void shouldGetAllGraphInfo() throws StoreException, OperationException {
        // Given
        FederatedStore store = new FederatedStore();
        final String graphId1 = "graph1";
        final Graph graph1 = FederatedTestUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);
        final GraphAccess access = new GraphAccess();

        // Init store and add graph
        store.initialise("federated", null, new StoreProperties());
        store.execute(
            new AddGraph.Builder()
                .graphConfig(graph1.getConfig())
                .schema(graph1.getSchema())
                .properties(graph1.getStoreProperties().getProperties()).build(),
            new Context());

        // Map of the expected values for the graph
        final Map<String, Object> graph1InfoExpected = Stream.of(
                new SimpleEntry<>(GetAllGraphInfoHandler.DESCRIPTION, graph1.getDescription()),
                new SimpleEntry<>(GetAllGraphInfoHandler.HOOKS, graph1.getConfig().getHooks()),
                new SimpleEntry<>(GetAllGraphInfoHandler.OP_DECLARATIONS, graph1.getStoreProperties().getOperationDeclarations().getOperations()),
                new SimpleEntry<>(GetAllGraphInfoHandler.STORE_CLASS, graph1.getStoreProperties().getStoreClass()),
                new SimpleEntry<>(GetAllGraphInfoHandler.PROPERTIES, graph1.getStoreProperties().getProperties()),
                new SimpleEntry<>(GetAllGraphInfoHandler.OWNER, access.getOwner()),
                new SimpleEntry<>(GetAllGraphInfoHandler.IS_PUBLIC, access.isPublic()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // When
        // Run a GetAllGraphInfo operation
        final Map<String, Object> graphInfo = store.execute(new GetAllGraphInfo(), new Context());

        // Then
        assertThat(graphInfo)
            .extracting(g -> g.get(graphId1))
            .usingRecursiveComparison()
            .isEqualTo(graph1InfoExpected);
    }

    @Test
    void shouldAddAndExecuteNamedOperationsFromFederatedCache() throws OperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId1 = "newGraph1";
        final String namedOpName = "customOp";

        // When
        final Graph federatedGraph = new Graph.Builder()
            .config(new GraphConfig(federatedGraphId))
            .store(new FederatedStore())
            .storeProperties(new FederatedStoreProperties())
            .build();

        // AddGraph operation
        final AddGraph addGraph1 = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId1))
                .schema(new Schema())
                .properties(new MapStoreProperties().getProperties())
                .build();

        federatedGraph.execute(addGraph1, new Context());

        // Add a new named operation to federated store
        final AddNamedOperation addNamedOp = new AddNamedOperation.Builder()
            .name(namedOpName)
            .operationChain(new OperationChain.Builder()
                .first(new GetGraphCreatedTime())
                .build())
            .build();

        // It should be added to the federated cache so its available to all graphs
        federatedGraph.execute(addNamedOp, new Context());

        // Attempt to execute the named operation it should be resolved before sending to sub graphs
        final NamedOperation<Void, Map<String, String>> runNamedOpOnSubGraph = new NamedOperation<>();
        runNamedOpOnSubGraph.setOperationName(namedOpName);
        runNamedOpOnSubGraph.addOption(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1);

        final NamedOperation<Void, Map<String, String>> runNamedOpOnFederated = new NamedOperation<>();
        runNamedOpOnFederated.setOperationName(namedOpName);

        // Make sure the named operation can be ran on sub graph or federated store directly
        Map<String, String> subGraphResult = federatedGraph.execute(runNamedOpOnSubGraph, new Context());
        Map<String, String> fedStoreResult = federatedGraph.execute(runNamedOpOnFederated, new Context());

        // Then
        assertThat(subGraphResult).containsOnlyKeys(graphId1);
        assertThat(fedStoreResult).containsOnlyKeys(federatedGraphId);
    }

    @Test
    void shouldAddAndExecuteNamedOperationsFromSubGraphCache() throws OperationException {
        // Given
        final String federatedGraphId = "federated";
        final String graphId1 = "newGraph1";
        final String namedOpName = "customOp";
        final Context context = new Context();

        // When
        final Graph federatedGraph = new Graph.Builder()
                .config(new GraphConfig(federatedGraphId))
                .store(new FederatedStore())
                .storeProperties(new FederatedStoreProperties())
                .build();

        // AddGraph operation
        final AddGraph addGraph1 = new AddGraph.Builder()
                .graphConfig(new GraphConfig(graphId1))
                .schema(new Schema())
                .properties(new MapStoreProperties().getProperties())
                .build();

        federatedGraph.execute(addGraph1, new Context());

        // Add a new named operation
        final AddNamedOperation addNamedOp = new AddNamedOperation.Builder()
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1)
            .name(namedOpName)
            .operationChain(new OperationChain.Builder()
                    .first(new GetGraphCreatedTime())
                    .build())
            .build();

        // The named op should be added to the sub graph cache only so
        // the federated store won't be able to resolve it locally
        federatedGraph.execute(addNamedOp, new Context());

        // Attempt to execute the named operation
        // It shouldn't be resolved before sending so only specific sub graphs can execute it
        final NamedOperation<Void, Map<String, String>> runNamedOpOnSubGraph = new NamedOperation<>();
        runNamedOpOnSubGraph.setOperationName(namedOpName);
        runNamedOpOnSubGraph.addOption(FederatedOperationHandler.OPT_GRAPH_IDS, graphId1);

        final NamedOperation<Void, Map<String, String>> runNamedOpOnFederated = new NamedOperation<>();
        runNamedOpOnFederated.setOperationName(namedOpName);

        Map<String, String> subGraphResult = federatedGraph.execute(runNamedOpOnSubGraph, new Context());

        // Then
        assertThat(subGraphResult).containsOnlyKeys(graphId1);
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> federatedGraph.execute(runNamedOpOnFederated, context))
            .withMessageContaining(namedOpName);
    }

}
