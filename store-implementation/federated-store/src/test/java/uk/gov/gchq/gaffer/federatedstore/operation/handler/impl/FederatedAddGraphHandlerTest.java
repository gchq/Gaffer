/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraphWithHooks;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.predicate.AdaptedPredicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.store.StoreProperties.OPERATION_DECLARATIONS_JSON;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedAddGraphHandlerTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String EXPECTED_GRAPH_ID_2 = "testGraphID2";
    private static final String CACHE_SERVICE_CLASS_STRING = HashMapCacheService.class.getCanonicalName();
    private User testUser;
    private User authUser;
    private User blankUser;
    private FederatedStore store;
    private FederatedStoreProperties federatedStoreProperties;

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(FederatedAddGraphHandlerTest.class, "properties/singleUseAccumuloStore.properties"));

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        this.store = new FederatedStore();
        federatedStoreProperties = getFederatedStorePropertiesWithHashMapCache();

        testUser = testUser();
        authUser = authUser();
        blankUser = blankUser();
    }

    @AfterEach
    void afterEach() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldAddGraph() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        final Schema expectedSchema = new Schema.Builder().build();

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new AddGraph());

        assertThat(graphs).hasSize(1);
        final GraphSerialisable next = graphs.iterator().next();
        assertThat(next.getGraphId()).isEqualTo(EXPECTED_GRAPH_ID);
        assertThat(next.getSchema()).isEqualTo(expectedSchema);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null, new AddGraph());

        assertThat(graphs).hasSize(2);
        final Iterator<GraphSerialisable> iterator = graphs.iterator();
        final HashSet<String> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }
        assertThat(set).contains(EXPECTED_GRAPH_ID, EXPECTED_GRAPH_ID_2);
    }

    @Test
    public void shouldAddGraphUsingLibrary() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final Schema expectedSchema = new Schema.Builder().build();

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);
        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new AddGraph());

        assertThat(graphs).hasSize(1);
        final GraphSerialisable next = graphs.iterator().next();
        assertThat(next.getGraphId()).isEqualTo(EXPECTED_GRAPH_ID);
        assertThat(next.getSchema()).isEqualTo(expectedSchema);

        final GraphLibrary library = new HashMapGraphLibrary();
        library.add(EXPECTED_GRAPH_ID_2, expectedSchema, PROPERTIES);
        store.setGraphLibrary(library);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null, new AddGraph());

        assertThat(graphs).hasSize(2);
        final Iterator<GraphSerialisable> iterator = graphs.iterator();
        final HashSet<String> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }

        assertThat(set).contains(EXPECTED_GRAPH_ID, EXPECTED_GRAPH_ID_2);
    }

    @Test
    public void shouldThrowWhenOverwriteGraphIsDifferent() throws Exception {
        final Schema expectedSchema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", String.class)
                .build();

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> federatedAddGraphHandler.doOperation(
                        new AddGraph.Builder()
                                .graphId(EXPECTED_GRAPH_ID)
                                .schema(expectedSchema)
                                .schema(new Schema.Builder()
                                        .type("unusual", String.class)
                                        .build())
                                .storeProperties(PROPERTIES)
                                .build(),
                        new Context(testUser), store))
                .withMessageContaining(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, EXPECTED_GRAPH_ID));
    }

    @Test
    public void shouldThrowWhenOverwriteGraphIsSameAndAccessIsDifferent() throws Exception {
        final Schema expectedSchema = new Schema.Builder().build();

        store.initialise(FEDERATEDSTORE_GRAPH_ID, new Schema(), federatedStoreProperties);

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> federatedAddGraphHandler.doOperation(
                        new AddGraph.Builder()
                                .graphId(EXPECTED_GRAPH_ID)
                                .schema(expectedSchema)
                                .graphAuths("X")
                                .storeProperties(PROPERTIES)
                                .build(),
                        new Context(testUser),
                        store))
                .withMessageContaining(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, EXPECTED_GRAPH_ID));
    }

    @Test
    public void shouldAddGraphIDOnlyWithAuths() throws Exception {

        federatedStoreProperties.setCustomPropertyAuths("auth1,auth2");
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final Schema expectedSchema = new Schema.Builder().build();

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> federatedAddGraphHandler.doOperation(
                        new AddGraph.Builder()
                                .graphId(EXPECTED_GRAPH_ID)
                                .schema(expectedSchema)
                                .storeProperties(PROPERTIES)
                                .build(),
                        new Context(testUser), store))
                .withMessageContaining(String.format(FederatedAddGraphHandler.USER_IS_LIMITED_TO_ONLY_USING_PARENT_PROPERTIES_ID_FROM_GRAPHLIBRARY_BUT_FOUND_STORE_PROPERTIES_S, ""));

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(authUser),
                store);

        final Collection<GraphSerialisable> graphs = store.getGraphs(authUser, null, new AddGraph());
        assertThat(graphs).hasSize(1);
        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);
        assertThat(graphs.iterator().next().getGraphId()).isEqualTo(EXPECTED_GRAPH_ID);
    }

    /**
     * Replicating a bug condition when setting auths the
     * FederatedAddGraphHandler didn't set the adding user.
     *
     * @throws Exception if Exception
     */
    @Test
    public void shouldAddGraphWithAuthsAndOwningUser() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final Schema expectedSchema = loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON);

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        new FederatedAddGraphHandler().doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .graphAuths("testAuth")
                        .build(),
                new Context(testUser),
                store);

        final Iterable<? extends Element> elements = new FederatedOutputIterableHandler<GetAllElements, Element>().doOperation(
                new GetAllElements(),
                new Context(testUser),
                store);

        assertThat(elements).isNotNull();
    }

    @Test
    public void shouldAddGraphAndAddSupportedOperations() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        final Schema expectedSchema = new Schema.Builder().build();

        assertThat(store.isSupported(GetElementsInRanges.class))
                .withFailMessage("Empty FederatedStore should NOT support GetElementsInRanges").isFalse();
        assertThat(store.isSupported(AddElementsFromHdfs.class))
                .withFailMessage("Empty FederatedStore should NOT support AddElementsFromHdfs").isFalse();

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .build(),
                new Context(testUser),
                store);

        assertThat(store.isSupported(GetElementsInRanges.class))
                .withFailMessage("FederatedStore with an added Accumulo store should support GetElementsInRanges").isTrue();
        assertThat(store.isSupported(AddElementsFromHdfs.class))
                .withFailMessage("FederatedStore with an added Accumulo store should support AddElementsFromHdfs").isTrue();
    }

    @Test
    public void shouldAddGraphWithCustomReadAccessPredicate() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final Schema expectedSchema = new Schema.Builder().build();

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final AccessPredicate allowBlankUserAndTestUserReadAccess = new AccessPredicate(new AdaptedPredicate(
                new CallMethod("getUserId"),
                new Or<>(new IsEqual(testUser.getUserId()), new IsEqual(blankUser.getUserId()))));

        new FederatedAddGraphHandler().doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(PROPERTIES)
                        .readAccessPredicate(allowBlankUserAndTestUserReadAccess)
                        .build(),
                new Context(testUser),
                store);

        assertThat(store.getGraphs(blankUser, null, new AddGraph())).hasSize(1);
        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(1);
    }

    /**
     * Adding a Graph with Cache previously would erase the static
     * cache service used by FederatedStore in the same JVM.
     * loosing all graphs and cached info.
     *
     * @throws StoreException     Store.Initialise throws exception.
     * @throws OperationException addGraph throws exception.
     */
    @Test
    public void shouldAddGraphWithACache() throws StoreException, OperationException {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        final Schema expectedSchema = new Schema.Builder().build();

        assertThat(store.getGraphs(testUser, null, new AddGraph())).hasSize(0);

        final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        final AccumuloProperties properties = new AccumuloProperties();
        properties.setProperties(PROPERTIES.getProperties());
        properties.setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(properties)
                        .build(),
                new Context(testUser),
                store);

        Collection<GraphSerialisable> graphs = store.getGraphs(testUser, null, new AddGraph());

        assertThat(graphs).hasSize(1);
        final GraphSerialisable next = graphs.iterator().next();
        assertThat(next.getGraphId()).isEqualTo(EXPECTED_GRAPH_ID);
        assertThat(next.getSchema()).isEqualTo(expectedSchema);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID_2)
                        .schema(expectedSchema)
                        .storeProperties(properties)
                        .build(),
                new Context(testUser),
                store);

        graphs = store.getGraphs(testUser, null, new AddGraph());

        assertThat(graphs).hasSize(2);
        final Iterator<GraphSerialisable> iterator = graphs.iterator();
        final HashSet<String> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next().getGraphId());
        }
        assertThat(set).contains(EXPECTED_GRAPH_ID, EXPECTED_GRAPH_ID_2);
    }

    @Test
    public void shouldAddGraphWithHandler() throws Exception {
        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        final Schema expectedSchema = new Schema.Builder().build();

        final AccumuloProperties clone = PROPERTIES.clone();

        clone.set(OPERATION_DECLARATIONS_JSON, new String(JSONSerialiser.serialise(new OperationDeclarations.Builder()
                .declaration(new OperationDeclaration.Builder()
                        .handler(new TestErrorHandler())
                        .operation(GetAllElements.class)
                        .build())
                .build(), true)));

        new FederatedAddGraphHandler().doOperation(
                new AddGraphWithHooks.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(expectedSchema)
                        .storeProperties(clone)
                        .hooks()
                        .build(),
                new Context(testUser),
                store);

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new GetAllElements(), new Context(testUser)))
                .withStackTraceContaining(TestErrorHandler.TEST_ERROR_HANDLER_FLAG);
    }
}
