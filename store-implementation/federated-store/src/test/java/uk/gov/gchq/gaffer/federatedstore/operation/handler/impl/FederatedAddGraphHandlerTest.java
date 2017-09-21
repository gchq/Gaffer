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
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S;

public class FederatedAddGraphHandlerTest {

    public static final String GAFFER_FEDERATEDSTORE_CUSTOM_PROPERTIES_AUTHS = "gaffer.federatedstore.customPropertiesAuths";

    @Test
    public void shouldAddGraph() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");

        assertEquals(0, store.getGraphs(null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        Collection<Graph> graphs = store.getGraphs(null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId + "b")
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        graphs = store.getGraphs(null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        next = iterator.next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedGraphId + "b", iterator.next().getGraphId());
    }

    @Test
    public void shouldAddGraphUsingLibrary() throws Exception {

        FederatedStore store = new FederatedStore();


        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set(StoreProperties.STORE_CLASS, "uk.gov.gchq.gaffer.federatedstore.FederatedStore");
        storeProperties.set(StoreProperties.STORE_PROPERTIES_CLASS, "uk.gov.gchq.gaffer.store.StoreProperties");

        assertEquals(0, store.getGraphs(null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        Collection<Graph> graphs = store.getGraphs(null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        final GraphLibrary mock = Mockito.mock(GraphLibrary.class);
        final String graphIdB = expectedGraphId + "b";
        BDDMockito.given(mock.get(graphIdB)).willReturn(new Pair<>(expectedSchema, storeProperties));
        BDDMockito.given(mock.exists(graphIdB)).willReturn(true);
        store.setGraphLibrary(mock);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(graphIdB)
                        .build(),
                new Context(new User("TestUser")),
                store);

        graphs = store.getGraphs(null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        next = iterator.next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(graphIdB, iterator.next().getGraphId());

        Mockito.verify(mock, Mockito.times(3)).get(graphIdB);
    }

    @Test
    public void shouldNotOverwriteGraph() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");

        assertEquals(0, store.getGraphs(null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(new User("TestUser")),
                store);

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(expectedGraphId)
                            .schema(expectedSchema)
                            .storeProperties(storeProperties)
                            .build(),
                    new Context(new User("TestUser")),
                    store);
        } catch (final OverwritingException e) {
            assertEquals(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, "testGraphID"), e.getMessage());
        }

    }

    @Test
    public void shouldAddGraphIDOnlyWithAuths() throws Exception {


        final StoreProperties federatedProperties = new StoreProperties();
        federatedProperties.set(GAFFER_FEDERATEDSTORE_CUSTOM_PROPERTIES_AUTHS, "auth1,auth2");
        FederatedStore store = new FederatedStore();
        store.initialise("FederatedStore", null, federatedProperties);

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");

        assertEquals(0, store.getGraphs(null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(expectedGraphId)
                            .schema(expectedSchema)
                            .storeProperties(graphStoreProperties)
                            .build(),
                    new Context(new User("TestUser")),
                    store);
            fail("Exception not thrown");
        } catch (OperationException e) {
            assertEquals("User is limited to only using parentPropertiesId from the graphLibrary," +
                            " but found storeProperties:{gaffer.store.class=uk.gov.gchq.gaffer.federatedstore.FederatedStore}",
                    e.getMessage());
        }


        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(new User.Builder()
                        .userId("TestUser")
                        .opAuth("auth1")
                        .build()),
                store);

        assertEquals(1, store.getGraphs(null).size());
        assertEquals(expectedGraphId, store.getGraphs(null).iterator().next().getGraphId());
    }

}