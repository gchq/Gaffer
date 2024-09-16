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

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federated.simple.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils.StoreType;

import java.util.AbstractMap.SimpleEntry;
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

        final Graph graph1 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);
        final Graph graph2 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId2, StoreType.MAP);

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
    void shouldPreventMixOfFederatedAndCoreOperationsInChain() throws StoreException {
        // Given
        FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise("federated", null, new StoreProperties());

        OperationChain<Iterable<? extends Element>> mixedChain = new OperationChain.Builder()
            .first(new AddGraph.Builder()
                .graphConfig(new GraphConfig("dummy"))
                .schema(new Schema())
                .properties(new java.util.Properties())
                .build())
            .then(new GetAllElements.Builder()
                .build())
            .build();

        // When/Then
        assertThatExceptionOfType(OperationException.class)
            .isThrownBy(() -> federatedStore.execute(mixedChain, new Context()))
            .withMessageContaining("Please submit each type separately");
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
    void shouldGetAllGraphInfo() throws StoreException, OperationException {
        // Given
        FederatedStore store = new FederatedStore();
        final String graphId1 = "graph1";
        final Graph graph1 = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId1, StoreType.MAP);

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
                new SimpleEntry<>("graphDescription", graph1.getDescription()),
                new SimpleEntry<>("graphHooks", graph1.getConfig().getHooks()),
                new SimpleEntry<>("operationDeclarations", graph1.getStoreProperties().getOperationDeclarations().getOperations()),
                new SimpleEntry<>("storeProperties", graph1.getStoreProperties().getProperties()))
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

}
