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

package uk.gov.gchq.gaffer.federated.simple.util;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;

public final class FederatedTestUtils {

    private FederatedTestUtils() {
        // utility class
    }

    public static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties
        .loadStoreProperties("/map-store.properties");
    public static final AccumuloProperties ACCUMULO_STORE_PROPERTIES = AccumuloProperties
        .loadStoreProperties("/accumulo-store.properties");

    public static Graph getBlankGraphWithModernSchema(Class<?> clazz, String graphId, StoreType storeType) {
        return new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(graphId)
                    .description("Graph using the modern dataset")
                    .build())
            .storeProperties(getStoreProperties(storeType))
            .addSchemas(StreamUtil.openStreams(clazz, "/modern/schema"))
            .build();
    }

    /**
     * Available store types for sub graphs
     */
    public enum StoreType {
        MAP, ACCUMULO;
    }

    public static StoreProperties getStoreProperties(StoreType storeType) {
        switch (storeType) {
            case MAP:
                return MAP_STORE_PROPERTIES;
            case ACCUMULO:
                return ACCUMULO_STORE_PROPERTIES;
            default:
                throw new IllegalArgumentException("Unknown StoreType: " + storeType);
        }
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
    public static void addGraphWithElements(FederatedStore store, Graph graph, Element... elements) throws OperationException {
        // Add Graph operation
        final AddGraph addGraph = new AddGraph.Builder()
                .graphConfig(graph.getConfig())
                .schema(graph.getSchema())
                .properties(graph.getStoreProperties().getProperties())
                .build();

        // Add the graph
        store.execute(addGraph, new Context());

        // Add elements if required
        if (elements.length > 0) {
            final AddElements addGraphElements = new AddElements.Builder()
                    .input(elements)
                    .option(FederatedOperationHandler.OPT_GRAPH_IDS, graph.getGraphId())
                    .build();

            store.execute(addGraphElements, new Context());
        }

    }
}
