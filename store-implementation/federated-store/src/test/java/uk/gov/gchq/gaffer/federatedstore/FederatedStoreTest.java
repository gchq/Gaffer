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

package uk.gov.gchq.gaffer.federatedstore;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.integration.FederatedStoreITs;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.TEST_USER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedStoreTest {
    private static final String PATH_FEDERATED_STORE_PROPERTIES = "/properties/federatedStoreTest.properties";
    private static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    private static final String ACC_ID_1 = "mockAccGraphId1";
    private static final String MAP_ID_1 = "mockMapGraphId1";
    private static final String PATH_ACC_STORE_PROPERTIES = "properties/singleUseMockAccStore.properties";
    private static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMockMapStore.properties";
    private static final String PATH_MAP_STORE_PROPERTIES_ALT = "properties/singleUseMockMapStoreAlt.properties";
    private static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    private static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    private static final String PATH_INVALID = "nothing.json";
    private static final String EXCEPTION_NOT_THROWN = "exception not thrown";
    private static final String USER_ID = "testUser";
    private static final String PROPS_ID_1 = "PROPS_ID_1";
    public static final String UNUSUAL_KEY = "unusualKey";
    public static final String KEY_DOES_NOT_BELONG = UNUSUAL_KEY + " was added to " + PROPS_ID_1 + " it should not be there";
    private static final String SCHEMA_ID_1 = "SCHEMA_ID_1";
    private static final String ALL_USERS = FederatedStoreUser.ALL_USERS;
    private static final HashSet<String> GRAPH_AUTHS = Sets.newHashSet(ALL_USERS);
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String CACHE_SERVICE_NAME = "federatedStoreGraphs";
    private static User authUser;
    private static User testUser;

    FederatedStore store;
    private FederatedStoreProperties federatedProperties;

    @Before
    public void setUp() throws Exception {
        store = new FederatedStore();
        federatedProperties = new FederatedStoreProperties();
        federatedProperties.setGraphAuth(ACC_ID_1, ALL_USERS);
        federatedProperties.setGraphAuth(MAP_ID_1, ALL_USERS);
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
        authUser = authUser();
        testUser = testUser();
    }

    @Test
    public void shouldInitialiseWithCache() throws StoreException {
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        assertNotNull(CacheServiceLoader.getService());
    }

    @Test
    public void shouldThrowExceptionWithoutInitialisation() {
        // Given
        Graph graphToAdd = new Graph.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        CacheServiceLoader.shutdown();

        // When / Then
        try {
            store.addGraphs(null, TEST_USER, graphToAdd);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final StoreException e) {
            assertTrue(e.getMessage().contains("No cache has been set"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenInitialisedWithNoCacheClassInProperties() throws StoreException {
        // Given
        StoreProperties storeProperties = new StoreProperties();

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, storeProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final StoreException e) {
            assertTrue(e.getMessage().contains("No cache has been set, please check the property"));
        }
    }

    @Test
    public void shouldAddGraphsToCache() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        Graph graphToAdd = new Graph.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        // When
        store.addGraphs(null, TEST_USER, graphToAdd);

        // Then
        assertEquals(1, store.getGraphs(testUser, ACC_ID_1).size());

        // When
        Collection<Graph> storeGraphs = store.getGraphs(testUser, null);

        // Then
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1));
        assertTrue(storeGraphs.contains(graphToAdd));

        // When
        store = new FederatedStore();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1));
    }

    @Test
    public void shouldAddMultipleGraphsToCache() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        List<Graph> graphsToAdd = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            graphsToAdd.add(new Graph.Builder()
                    .config(new GraphConfig(ACC_ID_1 + i))
                    .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                    .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                    .build());
        }

        // When
        store.addGraphs(null, TEST_USER, graphsToAdd.toArray(new Graph[graphsToAdd.size()]));

        // Then
        for (int i = 0; i < 10; i++) {
            assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1 + i));
        }

        // When
        store = new FederatedStore();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        for (int i = 0; i < 10; i++) {
            assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1 + i));
        }
    }

    @Test
    public void shouldReuseGraphsAlreadyInCache() throws Exception {
        //Check cache is empty
        assertNull(CacheServiceLoader.getService());

        //initialise FedStore
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //add something so it will be in the cache
        Graph graphToAdd = new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        store.addGraphs(null, TEST_USER, graphToAdd);

        //check the store and the cache
        assertEquals(1, store.getAllGraphIds(testUser).size());
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(MAP_ID_1));

        //restart the store
        store = null;
        store = new FederatedStore();
        federatedProperties.setGraphIds(MAP_ID_1);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //check the graph is already in there from the cache
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(MAP_ID_1));
        assertEquals(1, store.getAllGraphIds(testUser).size());
    }

    @Test
    public void shouldLoadGraphsWithIds() throws Exception {
        // Given
        federatedProperties.setGraphIds(ACC_ID_1 + "," + MAP_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);

        // When
        int before = store.getGraphs(testUser, null).size();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        Collection<Graph> graphs = store.getGraphs(testUser, null);
        int after = graphs.size();
        assertEquals(0, before);
        assertEquals(2, after);
        ArrayList<String> graphNames = Lists.newArrayList(ACC_ID_1, MAP_ID_1);
        for (Graph graph : graphs) {
            assertTrue(graphNames.contains(graph.getGraphId()));
        }
    }

    @Test
    public void shouldThrowErrorForFailedSchema() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_INVALID);

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Schema", "graphId: " + MAP_ID_1 + " schemaPath: " + PATH_INVALID), e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorForFailedProperty() throws Exception {
        //Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_INVALID);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);

        //When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Property", "graphId: " + MAP_ID_1 + " propertyPath: " + PATH_INVALID), e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorForIncompleteBuilder() throws Exception {
        //Given
        federatedProperties.setGraphIds(MAP_ID_1);

        //When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", "")));
        }
    }

    @Test
    public void shouldNotAllowOverwritingOfGraphWithFederatedScope() throws Exception {
        //Given
        federatedProperties.setGraphIds(ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When / Then
        try {
            store.addGraphs(null, null, new Graph.Builder()
                    .config(new GraphConfig(ACC_ID_1))
                    .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                    .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                    .build());
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertEquals(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE, ACC_ID_1), e.getMessage());
        }
    }

    @Test
    public void shouldDoUnhandledOperation() throws Exception {
        // When / Then
        try {
            store.doUnhandledOperation(null, null);
            fail(EXCEPTION_NOT_THROWN);
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldUpdateTraitsWhenNewGraphIsAdded() throws Exception {
        // Given
        federatedProperties.setGraphIds(ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Set<StoreTrait> before = store.getTraits();

        // When
        store.addGraphs(null, null, new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        //includes same as before but with more Traits
        Set<StoreTrait> after = store.getTraits();

        // Then
        assertEquals("Sole graph has 9 traits, so all traits of the federatedStore is 9", 9, before.size());
        assertEquals("the two graphs share 5 traits", 5, after.size());
        assertNotEquals(before, after);
        assertTrue(before.size() > after.size());
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsAdded() throws Exception {
        // Given
        federatedProperties.setGraphIds(ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Schema before = store.getSchema();

        // When
        store.addGraphs(null, null, new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build());
        Schema after = store.getSchema();

        // Then
        assertNotEquals(before, after);
    }

    @Test
    public void shouldUpdateTraitsToMinWhenGraphIsRemoved() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1 + "," + ACC_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When
        Set<StoreTrait> before = store.getTraits();
        store.remove(MAP_ID_1, testUser);
        Set<StoreTrait> after = store.getTraits();

        // Then
        assertEquals("Shared traits between the two graphs should be " + 5, 5, before.size());
        assertEquals("Shared traits counter-intuitively will go up after removing graph, because the sole remaining graph has 9 traits", 9, after.size());
        assertNotEquals(before, after);
        assertTrue(before.size() < after.size());
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsRemoved() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1 + "," + ACC_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Schema before = store.getSchema();

        // When
        store.remove(MAP_ID_1, testUser);
        Schema after = store.getSchema();

        // Then
        assertNotEquals(before, after);
    }

    @Test
    public void shouldFailWithIncompleteSchema() throws Exception {
        //Given
        federatedProperties.setGraphIds(ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, "/schema/edgeX2NoTypesSchema.json");

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", "")));
        }
    }

    @Test
    public void shouldTakeCompleteSchemaFromTwoFiles() throws Exception {
        // Given
        federatedProperties.setGraphIds(ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, "/schema/edgeX2NoTypesSchema.json" + ", /schema/edgeTypeSchema.json");
        int before = store.getGraphs(testUser, null).size();

        // When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        int after = store.getGraphs(testUser, null).size();

        // Then
        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        // Given
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        federatedProperties.setProperties(storeProperties.getProperties());
        int sizeBefore = store.getGraphs(authUser, null).size();

        // When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        int sizeAfter = store.getGraphs(authUser, null).size();

        // Then
        assertEquals(0, sizeBefore);
        assertEquals(2, sizeAfter);
    }

    @Test
    public void shouldCombineTraitsToMin() throws Exception {
        // Given
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        federatedProperties.setProperties(storeProperties.getProperties());
        HashSet<StoreTrait> traits = new HashSet<>();
        traits.addAll(SingleUseAccumuloStore.TRAITS);
        traits.retainAll(MapStore.TRAITS);
        Set<StoreTrait> before = store.getTraits();
        int sizeBefore = before.size();

        // When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Set<StoreTrait> after = store.getTraits();
        int sizeAfter = after.size();

        // Then
        assertEquals(5, MapStore.TRAITS.size());
        assertEquals(9, SingleUseAccumuloStore.TRAITS.size());
        assertNotEquals(SingleUseAccumuloStore.TRAITS, MapStore.TRAITS);
        assertEquals(0, sizeBefore);
        assertEquals(5, sizeAfter);
        assertEquals(traits, after);
    }

    @Test
    public void shouldContainNoElements() throws Exception {
        // Given
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        federatedProperties.setProperties(storeProperties.getProperties());
        // When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Set<Element> after = getElements();

        // Then
        assertEquals(0, after.size());
    }

    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        // Given
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        federatedProperties.setProperties(storeProperties.getProperties());
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        AddElements op = new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("BasicEdge")
                        .source("testSource")
                        .dest("testDest")
                        .property("property1", 12)
                        .build())
                .build();

        // When
        store.execute(op, new Context(authUser));

        // Then
        assertEquals(1, getElements().size());
    }

    @Test
    public void shouldReturnGraphIds() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1 + "," + ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);

        // When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Collection<String> allGraphIds = store.getAllGraphIds(testUser);

        // Then
        assertEquals(2, allGraphIds.size());
        assertTrue(allGraphIds.contains(ACC_ID_1));
        assertTrue(allGraphIds.contains(MAP_ID_1));

    }

    @Test
    public void shouldUpdateGraphIds() throws Exception {
        // Given
        federatedProperties.setGraphIds(ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When
        Collection<String> allGraphId = store.getAllGraphIds(testUser);

        // Then
        assertEquals(1, allGraphId.size());
        assertTrue(allGraphId.contains(ACC_ID_1));
        assertFalse(allGraphId.contains(MAP_ID_1));

        // When
        store.addGraphs(GRAPH_AUTHS, null, new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());
        Collection<String> allGraphId2 = store.getAllGraphIds(testUser);

        // Then
        assertEquals(2, allGraphId2.size());
        assertTrue(allGraphId2.contains(ACC_ID_1));
        assertTrue(allGraphId2.contains(MAP_ID_1));

        // When
        store.remove(ACC_ID_1, testUser);
        Collection<String> allGraphId3 = store.getAllGraphIds(testUser);

        // Then
        assertEquals(1, allGraphId3.size());
        assertFalse(allGraphId3.contains(ACC_ID_1));
        assertTrue(allGraphId3.contains(MAP_ID_1));

    }

    @Test
    public void shouldGetAllGraphIdsInUnmodifiableSet() throws Exception {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When / Then
        try {
            store.getAllGraphIds(testUser).add("newId");
            fail(EXCEPTION_NOT_THROWN);
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotUseSchema() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        final Schema unusedMock = Mockito.mock(Schema.class);

        // Then
        Mockito.verifyNoMoreInteractions(unusedMock);

        // When
        store.initialise(FEDERATED_STORE_ID, unusedMock, federatedProperties);
    }

    @Test
    public void shouldAddGraphFromLibrary() throws Exception {
        // Given
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build();
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.add(MAP_ID_1, schema, StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES)));
        store.setGraphLibrary(graphLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When
        final int before = store.getGraphs(testUser, null).size();
        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .build(), new Context(testUser));

        final int after = store.getGraphs(testUser, null).size();

        // Then
        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddNamedGraphFromGraphIDKeyButDefinedInLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphIds(MAP_ID_1);
        GraphLibrary library = new HashMapGraphLibrary();
        library.add(MAP_ID_1, new Schema.Builder()
                        .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                        .build(),
                StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES)));
        store.setGraphLibrary(library);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        final int after = store.getGraphs(testUser, null).size();
        assertEquals(1, after);
    }

    @Test
    public void shouldAddGraphFromGraphIDKeyButDefinedProperties() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        store.setGraphLibrary(graphLibrary);

        // When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        final int after = store.getGraphs(testUser, null).size();
        assertEquals(1, after);
    }

    @Test
    public void shouldAddNamedGraphsFromGraphIDKeyButDefinedInLibraryAndProperties() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1 + ", " + ACC_ID_1);
        federatedProperties.setGraphPropFile(ACC_ID_1, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(ACC_ID_1, PATH_BASIC_ENTITY_SCHEMA_JSON);
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build();
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.add(MAP_ID_1, schema, StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES)));

        // When
        store.setGraphLibrary(graphLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        final int after = store.getGraphs(testUser, null).size();
        assertEquals(2, after);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropId(MAP_ID_1, PROPS_ID_1);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.add(PROPS_ID_1, new Schema(), StoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES));

        // When
        store.setGraphLibrary(library);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertEquals(1, store.getGraphs(testUser, null).size());
        assertTrue(library.getProperties(PROPS_ID_1).equals(StoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES)));
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaId(MAP_ID_1, SCHEMA_ID_1);
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.openStream(FederatedStore.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build();
        library.add(SCHEMA_ID_1, schema, new StoreProperties());

        // When
        store.setGraphLibrary(library);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertEquals(1, store.getGraphs(testUser, null).size());
        assertTrue(library.getSchema(SCHEMA_ID_1).toString().equals(schema.toString()));
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropId(MAP_ID_1, PROPS_ID_1);
        federatedProperties.setGraphSchemaId(MAP_ID_1, SCHEMA_ID_1);
        final Schema schema = new Schema.Builder()
                .id(SCHEMA_ID_1)
                .json(StreamUtil.openStream(FederatedStore.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build();
        final StoreProperties properties = StoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES);
        properties.setId(PROPS_ID_1);
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.add(MAP_ID_1, schema, properties);

        // When
        store.setGraphLibrary(graphLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertEquals(1, store.getGraphs(testUser, null).size());
        assertTrue(JsonUtil.equals(graphLibrary.getSchema(SCHEMA_ID_1).toJson(false), schema.toJson(false)));
        assertTrue(graphLibrary.getProperties(PROPS_ID_1).equals(properties));
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibraryOverridden() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropId(MAP_ID_1, PROPS_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES_ALT);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        final MapStoreProperties prop = new MapStoreProperties();
        prop.setId(PROPS_ID_1);
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.addProperties(prop);
        assertFalse(KEY_DOES_NOT_BELONG, graphLibrary.getProperties(PROPS_ID_1).containsKey(UNUSUAL_KEY));

        // When
        store.setGraphLibrary(graphLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertEquals(1, store.getGraphs(testUser, null).size());
        assertFalse(KEY_DOES_NOT_BELONG, graphLibrary.getProperties(PROPS_ID_1).containsKey(UNUSUAL_KEY));
        assertEquals(prop.getProperties(), graphLibrary.getProperties(PROPS_ID_1).getProperties());
        assertTrue(store.getGraphs(testUser, null).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY) != null);

    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibraryOverridden() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.setGraphSchemaId(MAP_ID_1, SCHEMA_ID_1);
        final Schema schema = new Schema.Builder()
                .id(SCHEMA_ID_1)
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build();
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.addSchema(schema);

        // When
        store.setGraphLibrary(graphLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertEquals(1, store.getGraphs(testUser, null).size());
        assertTrue(store.getGraphs(testUser, null).iterator().next().getSchema().getEntityGroups().contains("BasicEntity"));
        assertTrue(graphLibrary.getSchema(SCHEMA_ID_1).equals(schema));
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibraryOverridden() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropId(MAP_ID_1, PROPS_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.setGraphSchemaId(MAP_ID_1, SCHEMA_ID_1);
        final MapStoreProperties prop = new MapStoreProperties();
        final String unusualKey = UNUSUAL_KEY;
        prop.set(unusualKey, "value");
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build();
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.add(MAP_ID_1, schema, prop);

        // When
        store.setGraphLibrary(graphLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Then
        assertEquals(1, store.getGraphs(testUser, null).size());
        assertTrue(store.getGraphs(testUser, null).iterator().next().getStoreProperties().getProperties().getProperty(unusualKey) != null);
        assertTrue(store.getGraphs(testUser, null).iterator().next().getSchema().getEntityGroups().contains("BasicEntity"));
        assertTrue(graphLibrary.getProperties(PROPS_ID_1).equals(prop));
        assertTrue(graphLibrary.getSchema(SCHEMA_ID_1).equals(schema));
    }

    @Test
    public void shouldNotAllowOverridingOfKnownGraphInLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES_ALT);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        final Schema schema = new Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES));
        graphLibrary.add(MAP_ID_1, schema, storeProperties);

        // When / Then
        try {
            store.setGraphLibrary(graphLibrary);
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getCause().getMessage().contains("GraphId " + MAP_ID_1 + " already exists with a different store properties:"));
        }
    }

    @Test
    public void shouldFederatedIfUserHasCorrectAuths() throws Exception {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        store.addGraphs(GRAPH_AUTHS, null, new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(MAP_ID_1)
                        .build())
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        // When
        final CloseableIterable<? extends Element> elements = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(USER_ID)
                        .opAuth(ALL_USERS)
                        .build()));

        // Then
        Assert.assertFalse(elements.iterator().hasNext());

        // When
        final CloseableIterable<? extends Element> x = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(USER_ID)
                        .opAuths("x")
                        .build()));

        // Then
        assertNull(x);
    }

    @Test
    public void shouldReturnSpecificGraphsFromCSVString() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        final List<Collection<Graph>> graphLists = populateGraphs(1, 2, 4);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(testUser, "mockGraphId1,mockGraphId2,mockGraphId4");

        // Then
        assertTrue(returnedGraphs.size() == 3);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    @Test
    public void shouldReturnNoGraphsFromEmptyString() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        final List<Collection<Graph>> graphLists = populateGraphs();
        final Collection<Graph> expectedGraphs = graphLists.get(0);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(testUser, "");

        // Then
        assertTrue(returnedGraphs.isEmpty());
        assertTrue(expectedGraphs.isEmpty());
    }

    @Test
    public void shouldReturnGraphsWithLeadingCommaString() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        final List<Collection<Graph>> graphLists = populateGraphs(2, 4);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(testUser, ",mockGraphId2,mockGraphId4");

        // Then
        assertTrue(returnedGraphs.size() == 2);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    @Test
    public void shouldAddGraphIdWithAuths() throws Exception {
        // Given
        federatedProperties.setFalseSkipFailedExecution();
        final HashMapGraphLibrary graphLibrary = new HashMapGraphLibrary();
        graphLibrary.add(MAP_ID_1,
                new Schema.Builder()
                        .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                        .build(),
                StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES)));

        final Graph fedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(FEDERATED_STORE_ID)
                        .library(graphLibrary)
                        .build())
                .addStoreProperties(federatedProperties)
                .build();

        // When
        int before = 0;
        for (String ignore : fedGraph.execute(
                new GetAllGraphIds(),
                authUser)) {
            before++;
        }

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphAuths("auth")
                        .graphId(MAP_ID_1)
                        .build(),
                authUser);


        int after = 0;
        for (String ignore : fedGraph.execute(
                new GetAllGraphIds(),
                authUser)) {
            after++;
        }


        fedGraph.execute(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group("BasicEntity")
                                .vertex("hi")
                                .build())
                        .build(),
                authUser);

        final CloseableIterable<? extends Element> elements = fedGraph.execute(
                new GetAllElements(),
                new User.Builder()
                        .userId(USER_ID + "Other")
                        .opAuth("auth")
                        .build());

        final CloseableIterable<? extends Element> x = fedGraph.execute(
                new GetAllElements(),
                new User.Builder()
                        .userId(USER_ID + "Other")
                        .opAuths("x")
                        .build());

        // Then
        assertEquals(0, before);
        assertEquals(1, after);
        Assert.assertNotNull(elements);
        Assert.assertTrue(elements.iterator().hasNext());
        assertNull(x);
    }

    @Test
    public void shouldThrowWithPropertiesErrorFromGraphLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropId(MAP_ID_1, PROPS_ID_1);
        federatedProperties.setGraphSchemaFile(MAP_ID_1, PATH_BASIC_EDGE_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "Something went wrong";
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenThrow(new IllegalArgumentException(error));

        store.setGraphLibrary(mockLibrary);

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (IllegalArgumentException e) {
            assertEquals(error, e.getCause().getMessage());
        }
        Mockito.verify(mockLibrary).getProperties(PROPS_ID_1);
    }

    @Test
    public void shouldThrowWithSchemaErrorFromGraphLibrary() throws Exception {
        // Given
        federatedProperties.setGraphIds(MAP_ID_1);
        federatedProperties.setGraphPropFile(MAP_ID_1, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.setGraphSchemaId(MAP_ID_1, SCHEMA_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "Something went wrong";
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (IllegalArgumentException e) {
            assertEquals(error, e.getCause().getMessage());
        }
        Mockito.verify(mockLibrary).getSchema(SCHEMA_ID_1);
    }

    @Test
    public void shouldReturnASingleGraph() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        final List<Collection<Graph>> graphLists = populateGraphs(1);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(testUser, "mockGraphId1");

        // Then
        assertTrue(returnedGraphs.size() == 1);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    private boolean checkUnexpected(final Collection<Graph> unexpectedGraphs, final Collection<Graph> returnedGraphs) {
        for (Graph graph : unexpectedGraphs) {
            if (returnedGraphs.contains(graph)) {
                return true;
            }
        }
        return false;
    }

    private List<Collection<Graph>> populateGraphs(int... expectedIds) throws StoreException {
        final Collection<Graph> expectedGraphs = new ArrayList<>();
        final Collection<Graph> unexpectedGraphs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Graph tempGraph = new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId("mockGraphId" + i)
                            .build())
                    .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                    .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                    .build();
            store.addGraphs(Sets.newHashSet(ALL_USERS), null, tempGraph);
            for (final int j : expectedIds) {
                if (i == j) {
                    expectedGraphs.add(tempGraph);
                }
            }
            if (!expectedGraphs.contains(tempGraph)) {
                unexpectedGraphs.add(tempGraph);
            }
        }
        final List<Collection<Graph>> graphLists = new ArrayList<>();
        graphLists.add(expectedGraphs);
        graphLists.add(unexpectedGraphs);
        return graphLists;
    }

    private Set<Element> getElements() throws uk.gov.gchq.gaffer.operation.OperationException {
        CloseableIterable<? extends Element> elements = store
                .execute(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema().getEdgeGroups())
                                .entities(store.getSchema().getEntityGroups())
                                .build())
                        .build(), new Context(authUser));

        return (null == elements) ? Sets.newHashSet() : Sets.newHashSet(elements);
    }
}
