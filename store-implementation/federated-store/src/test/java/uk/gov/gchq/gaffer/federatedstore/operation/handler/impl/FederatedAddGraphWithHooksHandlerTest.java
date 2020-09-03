/*
 * Copyright 2018-2020 Crown Copyright
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraphWithHooks;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.graph.hook.Log4jLogger;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedAddGraphWithHooksHandlerTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String EXPECTED_GRAPH_ID_2 = "testGraphID2";
    private static final String EXPECTED_GRAPH_ID_3 = "testGraphID3";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String EXCEPTION_EXPECTED = "Exception expected";
    private User testUser;
    private User authUser;
    private FederatedStore store;
    private FederatedStoreProperties federatedStoreProperties;
    private GetAllElements ignore;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @BeforeAll
    public static void setUpStore(@TempDir Path tempDir) {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        this.store = new FederatedStore();
        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        testUser = testUser();
        authUser = authUser();
        ignore = new IgnoreOptions();
    }

    @Test
    public void shouldAddGraph() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        FederatedAddGraphWithHooksHandler federatedAddGraphWithHooksHandler = new FederatedAddGraphWithHooksHandler();
        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null, ignore);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null, ignore);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        final HashSet<String> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }
        assertTrue(set.contains(EXPECTED_GRAPH_ID));
        assertTrue(set.contains(EXPECTED_GRAPH_ID_2));
    }

    @Test
    public void shouldAddGraphUsingLibrary() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());
        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        FederatedAddGraphWithHooksHandler federatedAddGraphWithHooksHandler = new FederatedAddGraphWithHooksHandler();
        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null, ignore);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        final GraphLibrary library = new HashMapGraphLibrary();
        library.add(EXPECTED_GRAPH_ID_3, expectedSchema, PROPERTIES);
        store.setGraphLibrary(library);

        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID_3)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null, ignore);

        assertEquals(2, graphs.size());
        Iterator<Graph> iterator = graphs.iterator();
        final HashSet<String> set = Sets.newHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }

        assertTrue(set.contains(EXPECTED_GRAPH_ID));
        assertTrue(set.contains(EXPECTED_GRAPH_ID_3));
    }

    @Test
    public void shouldThrowWhenOverwriteGraphIsDifferent() throws Exception {
        Schema expectedSchema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphWithHooksHandler federatedAddGraphWithHooksHandler = new FederatedAddGraphWithHooksHandler();

        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        try {
            federatedAddGraphWithHooksHandler.doOperation(
                    new AddGraphWithHooks.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .schema(new Schema.Builder()
                                    .type("unusual", String.class)
                                    .build())
                            .storeProperties(PROPERTIES)
                            .build(),
                    new Context(testUser),
                    store);
            fail(EXCEPTION_EXPECTED);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, EXPECTED_GRAPH_ID)));
        }
    }

    @Test
    public void shouldThrowWhenOverwriteGraphIsSameAndAccessIsDifferent() throws Exception {
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        FederatedAddGraphWithHooksHandler federatedAddGraphWithHooksHandler = new FederatedAddGraphWithHooksHandler();

        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        try {
            federatedAddGraphWithHooksHandler.doOperation(
                    new AddGraphWithHooks.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .graphAuths("X")
                            .storeProperties(PROPERTIES)
                            .build(),
                    new Context(testUser),
                    store);
            fail(EXCEPTION_EXPECTED);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, EXPECTED_GRAPH_ID)));
        }
    }

    @Test
    public void shouldAddGraphIDOnlyWithAuths() throws Exception {

        federatedStoreProperties.setCustomPropertyAuths("auth1,auth2");
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        FederatedAddGraphWithHooksHandler federatedAddGraphWithHooksHandler = new FederatedAddGraphWithHooksHandler();

        try {
            federatedAddGraphWithHooksHandler.doOperation(
                    new AddGraphWithHooks.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(expectedSchema)
                            .storeProperties(PROPERTIES)
                            .build(),
                    new Context(testUser),
                    store);
            fail(EXCEPTION_EXPECTED);
        } catch (OperationException e) {
            assertTrue(e.getMessage().contains(String.format(FederatedAddGraphWithHooksHandler.USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S, "")));
        }

        federatedAddGraphWithHooksHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(authUser),
                store);

        final Collection<Graph> graphs = store.getGraphs(authUser, null, ignore);
        assertEquals(1, graphs.size());
        assertEquals(0, store.getGraphs(testUser, null, ignore).size());
        assertEquals(EXPECTED_GRAPH_ID, graphs.iterator().next().getGraphId());
    }

    /**
     * Replicating a bug condition when setting auths the
     * FederatedAddGraphWithHooksHandler didn't set the adding user.
     *
     * @throws Exception if Exception
     */
    @Test
    public void shouldAddGraphWithAuthsAndAddingUser() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        new FederatedAddGraphWithHooksHandler().doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
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

    @Test
    public void shouldAddGraphWithHooks() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        Schema expectedSchema = new Schema.Builder().build();

        assertEquals(0, store.getGraphs(testUser, null, ignore).size());

        FederatedAddGraphWithHooksHandler federatedAddGraphHandler = new FederatedAddGraphWithHooksHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .hooks(new Log4jLogger())
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null, ignore);

        List<Class<? extends GraphHook>> graphHooks = graphs.iterator().next().getGraphHooks();
        assertTrue(graphHooks.contains(Log4jLogger.class));
    }

    private class IgnoreOptions extends GetAllElements {
        @Override
        public void setOptions(final Map<String, String> options) {
            //
        }
    }
}
