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

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedAddGraphHandlerTest {

    private User testUser;
    private User authUser;

    @Before
    public void setUp() throws Exception {
        testUser = testUser();
        authUser = authUser();
    }

    @Test
    public void shouldAddGraph() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new MapStoreProperties();

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

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
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        final HashSet<String> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }
        assertTrue(set.contains(expectedGraphId));
        assertTrue(set.contains(expectedGraphId + "b"));
    }

    @Test
    public void shouldAddGraphUsingLibrary() throws Exception {

        FederatedStore store = new FederatedStore();
        final FederatedStoreProperties fedProps = new FederatedStoreProperties();
        store.initialise("testFedGraph", null, fedProps);


        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties graphStoreProps = new AccumuloProperties();
        graphStoreProps.setStoreClass(SingleUseMockAccumuloStore.class);
        graphStoreProps.setStorePropertiesClass(AccumuloProperties.class);

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProps)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        final GraphLibrary mock = Mockito.mock(GraphLibrary.class);
        final String graphIdB = expectedGraphId + "b";
        BDDMockito.given(mock.get(graphIdB)).willReturn(new Pair<>(expectedSchema, graphStoreProps));
        BDDMockito.given(mock.exists(graphIdB)).willReturn(true);
        store.setGraphLibrary(mock);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(graphIdB)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        final HashSet<String> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }

        assertTrue(set.contains(expectedGraphId));
        assertTrue(set.contains(graphIdB));

        Mockito.verify(mock, Mockito.times(3)).get(graphIdB);
    }

    @Test
    public void shouldNotOverwriteGraph() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new MapStoreProperties();

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .build(),
                new Context(testUser),
                store);

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(expectedGraphId)
                            .schema(expectedSchema)
                            .storeProperties(storeProperties)
                            .build(),
                    new Context(testUser),
                    store);
        } catch (final OverwritingException e) {
            assertEquals(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, "testGraphID"), e.getMessage());
        }

    }

    @Test
    public void shouldAddGraphIDOnlyWithAuths() throws Exception {


        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCustomPropertyAuths("auth1,auth2");
        FederatedStore store = new FederatedStore();
        store.initialise("FederatedStore", null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        StoreProperties graphStoreProperties = new MapStoreProperties();
        graphStoreProperties.setStoreClass(MapStore.class);

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(expectedGraphId)
                            .schema(expectedSchema)
                            .storeProperties(graphStoreProperties)
                            .build(),
                    new Context(testUser),
                    store);
            fail("Exception not thrown");
        } catch (OperationException e) {
            assertTrue(e.getMessage().contains("User is limited to only using parentPropertiesId" +
                    " from the graphLibrary, but found storeProperties:" +
                    "{gaffer.store.class=uk.gov.gchq.gaffer.mapstore.MapStore"));
        }


        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(graphStoreProperties)
                        .build(),
                new Context(authUser),
                store);

        final Collection<Graph> graphs = store.getGraphs(authUser, null);
        assertEquals(1, graphs.size());
        assertEquals(0, store.getGraphs(testUser, null).size());
        assertEquals(expectedGraphId, graphs.iterator().next().getGraphId());
    }


    /**
     * Replicating a bug condition when setting auths the
     * FederatedAddGraphHandler didn't set the adding user.
     *
     * @throws Exception
     */
    @Test
    public void shouldAddGraphWithAuthsAndAddingUser() throws Exception {
        StoreProperties fedStoreProperties = new FederatedStoreProperties();

        FederatedStore store = new FederatedStore();
        store.initialise("testFedStore", null, fedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null).size());

        AccumuloProperties storeProperties = new AccumuloProperties();
        storeProperties.setStorePropertiesClass(AccumuloProperties.class);
        storeProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        new FederatedAddGraphHandler().doOperation(
                new AddGraph.Builder()
                        .graphId("testGraphID")
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .graphAuths("testAuth")
                        .build(),
                new Context(testUser),
                store);


        final CloseableIterable<? extends Element> elements = new FederatedGetAllElementsHandler().doOperation(
                new GetAllElements(),
                new Context(testUser),
                store);

        assertNotNull(elements);
    }
}