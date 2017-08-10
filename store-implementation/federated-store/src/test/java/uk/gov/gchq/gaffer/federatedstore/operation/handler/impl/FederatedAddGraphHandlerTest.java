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

import static org.junit.Assert.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S;

import org.junit.Test;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collection;
import java.util.Iterator;

public class FederatedAddGraphHandlerTest {


    @Test
    public void shouldAddGraph() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");

        assertEquals(0, store.getGraphs().size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .setGraphId(expectedGraphId)
                        .setSchema(expectedSchema)
                        .setStoreProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        Collection<Graph> graphs = store.getGraphs();

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .setGraphId(expectedGraphId + "b")
                        .setSchema(expectedSchema)
                        .setStoreProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        graphs = store.getGraphs();

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        next = iterator.next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedGraphId + "b", iterator.next().getGraphId());
    }

    @Test
    public void shouldNotOverwriteGraph() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");

        assertEquals(0, store.getGraphs().size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .setGraphId(expectedGraphId)
                        .setSchema(expectedSchema)
                        .setStoreProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .setGraphId(expectedGraphId)
                            .setSchema(expectedSchema)
                            .setStoreProperties(storeProperties)
                            .build(),
                    new Context(new User("TestUser")),
                    store);
        } catch (final OverwritingException e) {
            assertEquals(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, "testGraphID"), e.getMessage());
        }

    }
}