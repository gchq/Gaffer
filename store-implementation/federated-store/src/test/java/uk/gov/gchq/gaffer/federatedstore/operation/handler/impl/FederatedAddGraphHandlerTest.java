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

import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S;

public class FederatedAddGraphHandlerTest {

    private static final String GAFFER_FEDERATEDSTORE_CUSTOM_PROPERTIES_AUTHS = "gaffer.federatedstore.customPropertiesAuths";
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String EXPECTED_GRAPH_ID_2 = "testGraphID2";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String FEDERATEDSTORE_CLASS_STRING = "uk.gov.gchq.gaffer.federatedstore.FederatedStore";
    private static final String TEST_USER_ID = "testUser";
    private FederatedStore store;
    private StoreProperties federatedStoreProperties;

    @Before
    public void setUp() {
        this.store = new FederatedStore();
        federatedStoreProperties = new StoreProperties();
        federatedStoreProperties.set(StoreProperties.STORE_CLASS, FEDERATEDSTORE_CLASS_STRING);
        federatedStoreProperties.set(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);
    }

    @Test
    public void shouldAddGraph() throws Exception {
        Schema expectedSchema = new Schema.Builder().build();

        StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.set(StoreProperties.STORE_CLASS, FEDERATEDSTORE_CLASS_STRING);

        assertEquals(0, store.getGraphs(null).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(new User(TEST_USER_ID)),
                store);

        Collection<Graph> graphs = store.getGraphs(null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(new User(TEST_USER_ID)),
                store);

        graphs = store.getGraphs(null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        next = iterator.next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(EXPECTED_GRAPH_ID_2, iterator.next().getGraphId());
    }

    @Test
    public void shouldAddGraphUsingLibrary() throws Exception {
        Schema expectedSchema = new Schema.Builder().build();

        StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.set(StoreProperties.STORE_CLASS, FEDERATEDSTORE_CLASS_STRING);
        graphStoreProperties.set(StoreProperties.STORE_PROPERTIES_CLASS, "uk.gov.gchq.gaffer.store.StoreProperties");

        assertEquals(0, store.getGraphs(null).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(new User(TEST_USER_ID)),
                store);

        Collection<Graph> graphs = store.getGraphs(null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        final GraphLibrary mock = Mockito.mock(GraphLibrary.class);
        final String graphId2 = EXPECTED_GRAPH_ID_2;
        BDDMockito.given(mock.get(graphId2)).willReturn(new Pair<>(expectedSchema, graphStoreProperties));
        BDDMockito.given(mock.exists(graphId2)).willReturn(true);
        store.setGraphLibrary(mock);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(graphId2)
                        .build(),
                new Context(new User(TEST_USER_ID)),
                store);

        graphs = store.getGraphs(null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        next = iterator.next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(graphId2, iterator.next().getGraphId());

        Mockito.verify(mock, Mockito.times(3)).get(graphId2);
    }

    @Test
    public void shouldNotOverwriteGraph() throws Exception {
        Schema expectedSchema = new Schema.Builder().build();

        StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.set(StoreProperties.STORE_CLASS, FEDERATEDSTORE_CLASS_STRING);

        assertEquals(0, store.getGraphs(null).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(new User(TEST_USER_ID)),
                store);

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .storeProperties(graphStoreProperties)
                            .build(),
                    new Context(new User(TEST_USER_ID)),
                    store);
        } catch (final OverwritingException e) {
            assertEquals(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, EXPECTED_GRAPH_ID), e.getMessage());
        }

    }

    @Test
    public void shouldAddGraphIDOnlyWithAuths() throws Exception {
        federatedStoreProperties.set(GAFFER_FEDERATEDSTORE_CUSTOM_PROPERTIES_AUTHS, "auth1,auth2");

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        StoreProperties graphStoreProperties = new StoreProperties();
        graphStoreProperties.set(StoreProperties.STORE_CLASS, FEDERATEDSTORE_CLASS_STRING);

        assertEquals(0, store.getGraphs(null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .storeProperties(graphStoreProperties)
                            .build(),
                    new Context(new User(TEST_USER_ID)),
                    store);
            fail("Exception not thrown");
        } catch (OperationException e) {
            assertEquals("User is limited to only using parentPropertiesId from the graphLibrary," +
                            " but found storeProperties:{gaffer.store.class=uk.gov.gchq.gaffer.federatedstore.FederatedStore}",
                    e.getMessage());
        }

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(new User.Builder()
                        .userId(TEST_USER_ID)
                        .opAuth("auth1")
                        .build()),
                store);

        assertEquals(1, store.getGraphs(null).size());
        assertEquals(EXPECTED_GRAPH_ID, store.getGraphs(null).iterator().next().getGraphId());
    }


    /**
     * Replicating a bug condition when setting auths the
     * FederatedAddGraphHandler didn't set the adding user.
     *
     * @throws Exception
     */
    @Test
    public void shouldAddGraphWithAuthsAndAddingUser() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(null).size());

        AccumuloProperties graphStoreProperties = new AccumuloProperties();
        graphStoreProperties.setStorePropertiesClass(AccumuloProperties.class);
        graphStoreProperties.setStoreClass(MockAccumuloStore.class);
        graphStoreProperties.set(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);

        new FederatedAddGraphHandler().doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .graphAuths("testAuth")
                        .build(),
                new Context(new User(TEST_USER_ID)),
                store);


        final CloseableIterable<? extends Element> elements = new FederatedGetAllElementsHandler().doOperation(
                new GetAllElements(),
                new Context(new User.Builder()
                        .userId(TEST_USER_ID)
                        .build()),
                store);

        assertNotNull(elements);
    }
}