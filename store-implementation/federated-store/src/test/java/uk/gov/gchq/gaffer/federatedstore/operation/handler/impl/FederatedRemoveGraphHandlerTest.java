/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class FederatedRemoveGraphHandlerTest {

    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String FEDERATEDSTORE_CLASS_STRING = "uk.gov.gchq.gaffer.federatedstore.FederatedStore";
    private static final String TEST_USER = "testUser";

    @Test
    public void shouldRemoveGraph() throws Exception {
        FederatedStore store = new FederatedStore();

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set(StoreProperties.STORE_CLASS, FEDERATEDSTORE_CLASS_STRING);
        storeProperties.set(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), storeProperties);

        store.addGraphs(new Graph.Builder()
                .config(new GraphConfig(EXPECTED_GRAPH_ID))
                .addSchema(new Schema.Builder().build())
                .storeProperties(storeProperties)
                .build());

        assertEquals(1, store.getGraphs(null).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .setGraphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(new User(TEST_USER)),
                store);

        Collection<Graph> graphs = store.getGraphs(null);

        assertEquals(0, graphs.size());

    }
}