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

package uk.gov.gchq.gaffer.federated.simple.operation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils;
import uk.gov.gchq.gaffer.federated.simple.util.ModernDatasetUtils.StoreType;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ChangeGraphIdTest {
    private static final String FED_STORE_ID = "federated";
    private static final String NEW_GRAPH_ID = "newGraphId";
    private static final String GRAPH_ID_ERROR = "Graph with Graph ID: %s is not available to this federated store";

    @AfterEach
    void reset() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldChangeGraphIdAndPreserveData() throws StoreException, OperationException, CacheOperationException {
        // Given
        final String graphId = "shouldChangeGraphIdAndPreserveData";
        final Graph originalGraph = ModernDatasetUtils.getBlankGraphWithModernSchema(this.getClass(), graphId, StoreType.ACCUMULO);

        // Elements to add to the graph
        final Properties graphEntityProps = new Properties();
        graphEntityProps.put("name", "marko");
        final Entity graphEntity = new Entity("person", "1", graphEntityProps);

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(FED_STORE_ID, null, new StoreProperties());
        // Add elements to the graph
        addGraphWithElements(federatedStore, originalGraph, graphEntity);
        GraphSerialisable expectedGraphSerialisable = federatedStore.getGraph(graphId);

        // Change graph ID operation
        final ChangeGraphId changeGraphId = new ChangeGraphId.Builder()
            .graphId(graphId)
            .newGraphId(NEW_GRAPH_ID)
            .build();
        federatedStore.execute(changeGraphId, new Context());

        // Check that the config + properties remains the same
        assertThat(federatedStore.getGraph(NEW_GRAPH_ID))
            .satisfies(gs -> {
                assertThat(gs.getConfig())
                    .usingRecursiveComparison()
                    .ignoringFields("graphId")
                    .isEqualTo(expectedGraphSerialisable.getConfig());

                assertThat(gs.getStoreProperties())
                    .isEqualTo(expectedGraphSerialisable.getStoreProperties());
            });

        // Ensure that there are no references to the 'old' graph ID
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> federatedStore.getGraph(graphId))
            .withMessage(String.format(GRAPH_ID_ERROR, graphId));

        // See if the graph data still exists in the cluster
        OperationChain<Iterable<? extends Element>> getAllElements = new OperationChain.Builder()
            .first(new GetAllElements.Builder()
                    .build())
            .option(FederatedOperationHandler.OPT_GRAPH_IDS, NEW_GRAPH_ID)
            .build();
        Iterable<? extends Element> resultGetAll = federatedStore.execute(getAllElements, new Context());

        assertThat(resultGetAll).extracting(e -> (Element) e).containsExactly(graphEntity);
    }

    @Test
    void shouldThrowErrorChangingIdIfGraphDoesntExist() throws StoreException {
        // Given
        final String graphId = "shouldThrowErrorChangingIdIfGraphDoesntExist";
        final ChangeGraphId changeGraphId = new ChangeGraphId.Builder()
            .graphId(graphId)
            .newGraphId(NEW_GRAPH_ID)
            .build();
        final Context context = new Context();

        // When
        final FederatedStore federatedStore = new FederatedStore();
        federatedStore.initialise(FED_STORE_ID, null, new StoreProperties());

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> federatedStore.execute(changeGraphId, context))
                .withMessageContaining(String.format(GRAPH_ID_ERROR, graphId));
    }

     /**
     * Adds a given graph and elements to a federated store.
     *
     * @param store The federated store
     * @param graph The graph to add
     * @param elements The elements to add to the graph
     *
     * @throws OperationException If fails
     */
    private void addGraphWithElements(FederatedStore store, Graph graph, Element... elements) throws OperationException {
        // Add Graph operation
        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(graph.getConfig())
                .schema(graph.getSchema())
                .properties(graph.getStoreProperties().getProperties())
                .build();

        // Add elements operation
        final AddElements addGraphElements = new AddElements.Builder()
                .input(elements)
                .option(FederatedOperationHandler.OPT_GRAPH_IDS, graph.getGraphId())
                .build();

        // Add the graph and its elements
        store.execute(addGraph, new Context());
        store.execute(addGraphElements, new Context());
    }
}
