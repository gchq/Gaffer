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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
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
import java.util.Arrays;
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
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.TEST_USER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler.NO_RESULTS_TO_MERGE_ERROR;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;

public class FederatedStoreTest {
    public static final String ID_SCHEMA_ENTITY = "basicEntitySchema";
    public static final String ID_SCHEMA_EDGE = "basicEdgeSchema";
    public static final String ID_PROPS_ACC = "mockAccProps";
    public static final String ID_PROPS_MAP = "mockMapProps";
    public static final String INVALID = "invalid";
    public static final String ID_PROPS_MAP_ALT = "mockMapPropsAlt";
    private static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    private static final String ACC_ID_1 = "mockAccGraphId1";
    private static final String MAP_ID_1 = "mockMapGraphId1";
    private static final String PATH_ACC_STORE_PROPERTIES = "properties/singleUseMockAccStore.properties";
    private static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMockMapStore.properties";
    private static final String PATH_MAP_STORE_PROPERTIES_ALT = "properties/singleUseMockMapStoreAlt.properties";
    private static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    private static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    private static final String EXCEPTION_NOT_THROWN = "exception not thrown";
    private static final String USER_ID = "blankUser";
    public static final String UNUSUAL_KEY = "unusualKey";
    public static final String KEY_DOES_NOT_BELONG = UNUSUAL_KEY + " was added to " + ID_PROPS_MAP + " it should not be there";
    private static final String ALL_USERS = FederatedStoreUser.ALL_USERS;
    private static final HashSet<String> GRAPH_AUTHS = Sets.newHashSet(ALL_USERS);
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String INVALID_CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.invalid";
    private static final String CACHE_SERVICE_NAME = "federatedStoreGraphs";
    public static final String PATH_INCOMPLETE_SCHEMA = "/schema/edgeX2NoTypesSchema.json";
    public static final String PATH_INCOMPLETE_SCHEMA_PART_2 = "/schema/edgeTypeSchema.json";
    private FederatedStore store;
    private FederatedStoreProperties federatedProperties;
    private HashMapGraphLibrary library;
    private Context userContext;
    private User blankUser;

    @Before
    public void setUp() throws Exception {
        clearCache();
        federatedProperties = new FederatedStoreProperties();
        federatedProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));

        clearLibrary();
        library = new HashMapGraphLibrary();
        library.addProperties(ID_PROPS_ACC, getPropertiesFromPath(PATH_ACC_STORE_PROPERTIES));
        library.addProperties(ID_PROPS_MAP, getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES));
        library.addProperties(ID_PROPS_MAP_ALT, getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES_ALT));
        library.addSchema(ID_SCHEMA_EDGE, getSchemaFromPath(PATH_BASIC_EDGE_SCHEMA_JSON));
        library.addSchema(ID_SCHEMA_ENTITY, getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON));

        store = new FederatedStore();
        store.setGraphLibrary(library);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        userContext = new Context(blankUser());
        blankUser = blankUser();
    }

    @After
    public void tearDown() throws Exception {
        assertEquals("Library has changed: " + ID_PROPS_ACC, library.getProperties(ID_PROPS_ACC), getPropertiesFromPath(PATH_ACC_STORE_PROPERTIES));
        assertEquals("Library has changed: " + ID_PROPS_MAP, library.getProperties(ID_PROPS_MAP), getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES));
        assertEquals("Library has changed: " + ID_PROPS_MAP_ALT, library.getProperties(ID_PROPS_MAP_ALT), getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES_ALT));
        assertEquals("Library has changed: " + ID_SCHEMA_EDGE, new String(library.getSchema(ID_SCHEMA_EDGE).toJson(false), CommonConstants.UTF_8), new String(getSchemaFromPath(PATH_BASIC_EDGE_SCHEMA_JSON).toJson(false), CommonConstants.UTF_8));
        assertEquals("Library has changed: " + ID_SCHEMA_ENTITY, new String(library.getSchema(ID_SCHEMA_ENTITY).toJson(false), CommonConstants.UTF_8), new String(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON).toJson(false), CommonConstants.UTF_8));
        clearLibrary();
        clearCache();
    }

    @Test
    public void shouldLoadGraphsWithIds() throws Exception {
        // When
        int before = store.getGraphs(blankUser, null).size();

        addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, ID_SCHEMA_EDGE);
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC, ID_SCHEMA_ENTITY);

        // Then
        Collection<Graph> graphs = store.getGraphs(blankUser, null);
        int after = graphs.size();
        assertEquals(0, before);
        assertEquals(2, after);
        ArrayList<String> graphNames = Lists.newArrayList(ACC_ID_1, MAP_ID_1);
        for (Graph graph : graphs) {
            assertTrue(graphNames.contains(graph.getGraphId()));
        }
    }

    @Test
    public void shouldThrowErrorForFailedSchemaID() throws Exception {
        // When / Then
        try {
            addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, INVALID);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e.getCause(), SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, Arrays.toString(new String[]{INVALID}));
        }
    }

    @Test
    public void shouldThrowErrorForFailedPropertyID() throws Exception {
        //When / Then
        try {
            addGraphWithIds(MAP_ID_1, INVALID, ID_SCHEMA_EDGE);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e.getCause(), STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, INVALID);
        }
    }

    @Test
    public void shouldThrowErrorForMissingProperty() throws Exception {
        //When / Then
        try {
            ArrayList<String> schemas = Lists.newArrayList(ID_SCHEMA_EDGE);
            store.execute(new AddGraph.Builder()
                    .graphId(MAP_ID_1)
                    .isPublic(true)
                    .parentSchemaIds(schemas)
                    .build(), userContext);
            fail("a graph was created without a defined properties");
        } catch (final Exception e) {
            assertContains(e.getCause(), GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, MAP_ID_1, "StoreProperties");
        }
    }

    @Test
    public void shouldThrowErrorForMissingSchema() throws Exception {
        //When / Then
        try {
            store.execute(new AddGraph.Builder()
                    .graphId(MAP_ID_1)
                    .isPublic(true)
                    .parentPropertiesId(ID_PROPS_MAP)
                    .build(), userContext);
            fail("a graph was created without a defined schema");
        } catch (final Exception e) {
            assertContains(e.getCause(), GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, MAP_ID_1, "Schema");
        }
    }

    @Test
    public void shouldNotAllowOverwritingOfGraphWithinFederatedScope() throws Exception {
        //Given
        addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, ID_SCHEMA_ENTITY);

        // When / Then
        try {
            addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, ID_SCHEMA_EDGE);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e, "User is attempting to overwrite a graph");
            assertContains(e, "GraphId: ", MAP_ID_1);
        }

        // When / Then
        try {
            addGraphWithIds(MAP_ID_1, ID_PROPS_MAP_ALT, ID_SCHEMA_ENTITY);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e, "User is attempting to overwrite a graph");
            assertContains(e, "GraphId: ", MAP_ID_1);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldDoUnhandledOperation() throws Exception {
        store.doUnhandledOperation(null, null);
    }

    @Test
    public void shouldAlwaysReturnSupportedTraits() throws Exception {
        // Given
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC, ID_SCHEMA_ENTITY);

        Set<StoreTrait> before = store.getTraits();

        // When
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);

        Set<StoreTrait> after = store.getTraits();
        assertEquals(StoreTrait.values().length, before.size());
        assertEquals(StoreTrait.values().length, after.size());
        assertEquals(before, after);
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsAdded() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);
        Schema before = store.getSchema((Operation) null, blankUser);
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);
        Schema after = store.getSchema((Operation) null, blankUser);
        // Then
        assertNotEquals(before, after);
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsRemoved() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);
        Schema was = store.getSchema((Operation) null, blankUser);
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);

        Schema before = store.getSchema((Operation) null, blankUser);

        // When
        store.remove(MAP_ID_1, blankUser);

        Schema after = store.getSchema((Operation) null, blankUser);
        assertNotEquals(before.toString(), after.toString());
        assertEquals(was.toString(), after.toString());
    }

    @Test
    public void shouldFailWithIncompleteSchema() throws Exception {
        // When / Then
        try {
            addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_INCOMPLETE_SCHEMA);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e, FederatedAddGraphHandler.ERROR_BUILDING_GRAPH_GRAPH_ID_S, ACC_ID_1);
        }
    }

    @Test
    public void shouldTakeCompleteSchemaFromTwoFiles() throws Exception {
        // Given
        int before = store.getGraphs(blankUser, null).size();
        addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_INCOMPLETE_SCHEMA, PATH_INCOMPLETE_SCHEMA_PART_2);

        // When
        int after = store.getGraphs(blankUser, null).size();

        // Then
        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        // Given
        int sizeBefore = store.getGraphs(blankUser, null).size();

        // When
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);
        addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);

        int sizeAfter = store.getGraphs(blankUser, null).size();

        // Then
        assertEquals(0, sizeBefore);
        assertEquals(2, sizeAfter);
    }

    @Test
    public void shouldContainNoElements() throws Exception {
        // When
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);
        Set<Element> after = getElements();

        // Then
        assertEquals(0, after.size());
    }

    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        // Given
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);

        AddElements op = new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("BasicEdge")
                        .source("testSource")
                        .dest("testDest")
                        .property("property1", 12)
                        .build())
                .build();

        // When
        store.execute(op, userContext);

        // Then
        assertEquals(1, getElements().size());
    }

    @Test
    public void shouldReturnGraphIds() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);

        // When
        Collection<String> allGraphIds = store.getAllGraphIds(blankUser);

        // Then
        assertEquals(2, allGraphIds.size());
        assertTrue(allGraphIds.contains(ACC_ID_1));
        assertTrue(allGraphIds.contains(MAP_ID_1));

    }

    @Test
    public void shouldUpdateGraphIds() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PATH_ACC_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);


        // When
        Collection<String> allGraphId = store.getAllGraphIds(blankUser);

        // Then
        assertEquals(1, allGraphId.size());
        assertTrue(allGraphId.contains(ACC_ID_1));
        assertFalse(allGraphId.contains(MAP_ID_1));

        // When
        addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, ID_SCHEMA_ENTITY);
        Collection<String> allGraphId2 = store.getAllGraphIds(blankUser);

        // Then
        assertEquals(2, allGraphId2.size());
        assertTrue(allGraphId2.contains(ACC_ID_1));
        assertTrue(allGraphId2.contains(MAP_ID_1));

        // When
        store.remove(ACC_ID_1, blankUser);
        Collection<String> allGraphId3 = store.getAllGraphIds(blankUser);

        // Then
        assertEquals(1, allGraphId3.size());
        assertFalse(allGraphId3.contains(ACC_ID_1));
        assertTrue(allGraphId3.contains(MAP_ID_1));

    }

    @Test
    public void shouldGetAllGraphIdsInUnmodifiableSet() throws Exception {
        // Given
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);

        // When / Then
        Collection<String> allGraphIds = store.getAllGraphIds(blankUser);
        try {
            allGraphIds.add("newId");
            fail(EXCEPTION_NOT_THROWN);
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }

        try {
            allGraphIds.remove("newId");
            fail(EXCEPTION_NOT_THROWN);
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotUseSchema() throws Exception {
        // Given
        final Schema unusedMock = Mockito.mock(Schema.class);
        // When
        store.initialise(FEDERATED_STORE_ID, unusedMock, federatedProperties);
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);
        // Then
        Mockito.verifyNoMoreInteractions(unusedMock);
    }

    @Test
    public void shouldAddGraphFromLibrary() throws Exception {
        // Given
        library.add(MAP_ID_1, library.getSchema(ID_SCHEMA_ENTITY), library.getProperties(ID_PROPS_MAP));

        // When
        final int before = store.getGraphs(blankUser, null).size();
        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .build(), new Context(blankUser));

        final int after = store.getGraphs(blankUser, null).size();

        // Then
        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibrary() throws Exception {
        // When
        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .parentPropertiesId(ID_PROPS_MAP)
                .isPublic(true)
                .schema(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build(), userContext);


        // Then
        assertEquals(1, store.getGraphs(blankUser, null).size());
        assertTrue(library.getProperties(ID_PROPS_MAP).equals(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES)));
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibrary() throws Exception {
        // When
        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .storeProperties(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES))
                .isPublic(true)
                .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_ENTITY))
                .build(), userContext);


        // Then
        assertEquals(1, store.getGraphs(blankUser, null).size());
        assertTrue(library.getSchema(ID_SCHEMA_ENTITY).toString().equals(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON).toString()));
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibrary() throws Exception {
        // When
        addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, ID_SCHEMA_ENTITY);

        // Then
        assertEquals(1, store.getGraphs(blankUser, null).size());
        Graph graph = store.getGraphs(blankUser, MAP_ID_1).iterator().next();
        assertEquals(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON).toString(), graph.getSchema().toString());
        assertEquals(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES), graph.getStoreProperties());

    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibraryOverridden() throws Exception {
        // Given
        assertFalse(KEY_DOES_NOT_BELONG, library.getProperties(ID_PROPS_MAP).containsKey(UNUSUAL_KEY));

        // When
        Builder schema = new Builder();
        for (String path : new String[]{PATH_BASIC_ENTITY_SCHEMA_JSON}) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .storeProperties(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES_ALT))
                .parentPropertiesId(ID_PROPS_MAP)
                .isPublic(true)
                .schema(schema.build())
                .build(), userContext);

        // Then
        assertEquals(1, store.getGraphs(blankUser, null).size());
        assertTrue(store.getGraphs(blankUser, null).iterator().next().getStoreProperties().containsKey(UNUSUAL_KEY));
        assertFalse(KEY_DOES_NOT_BELONG, library.getProperties(ID_PROPS_MAP).containsKey(UNUSUAL_KEY));
        assertTrue(store.getGraphs(blankUser, null).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY) != null);

    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibraryOverridden() throws Exception {
        ArrayList<String> schemas = Lists.newArrayList(ID_SCHEMA_ENTITY);
        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .isPublic(true)
                .schema(getSchemaFromPath(PATH_BASIC_EDGE_SCHEMA_JSON))
                .parentSchemaIds(schemas)
                .parentPropertiesId(ID_PROPS_MAP)
                .build(), userContext);

        // Then
        assertEquals(1, store.getGraphs(blankUser, null).size());
        assertTrue(store.getGraphs(blankUser, null).iterator().next().getSchema().getEntityGroups().contains("BasicEntity"));
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibraryOverridden() throws Exception {
        // Given
        assertFalse(KEY_DOES_NOT_BELONG, library.getProperties(ID_PROPS_MAP).containsKey(UNUSUAL_KEY));

        // When
        Builder tempSchema = new Builder();
        for (String path : new String[]{PATH_BASIC_EDGE_SCHEMA_JSON}) {
            tempSchema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .isPublic(true)
                .storeProperties(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES_ALT))
                .parentPropertiesId(ID_PROPS_MAP)
                .schema(tempSchema.build())
                .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_ENTITY))
                .build(), userContext);

        // Then
        assertEquals(1, store.getGraphs(blankUser, null).size());
        assertTrue(store.getGraphs(blankUser, null).iterator().next().getStoreProperties().containsKey(UNUSUAL_KEY));
        assertFalse(KEY_DOES_NOT_BELONG, library.getProperties(ID_PROPS_MAP).containsKey(UNUSUAL_KEY));
        assertTrue(store.getGraphs(blankUser, null).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY) != null);
        assertTrue(store.getGraphs(blankUser, null).iterator().next().getSchema().getEntityGroups().contains("BasicEntity"));
    }

    @Test
    public void shouldNotAllowOverridingOfKnownGraphInLibrary() throws Exception {
        // Given
        library.add(MAP_ID_1, getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON), getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES));

        // When / Then
        try {
            store.execute(new AddGraph.Builder()
                    .graphId(MAP_ID_1)
                    .parentPropertiesId(ID_PROPS_MAP_ALT)
                    .isPublic(true)
                    .build(), userContext);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e.getCause(), "Graph: " + MAP_ID_1 + " already exists so you cannot use a different StoreProperties");
        }

        // When / Then
        try {
            store.execute(new AddGraph.Builder()
                    .graphId(MAP_ID_1)
                    .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_EDGE))
                    .isPublic(true)
                    .build(), userContext);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertContains(e.getCause(), "Graph: " + MAP_ID_1 + " already exists so you cannot use a different Schema");
        }
    }

    @Test
    public void shouldFederatedIfUserHasCorrectAuths() throws Exception {
        // Given

        store.addGraphs(GRAPH_AUTHS, null, false, new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(MAP_ID_1)
                        .build())
                .storeProperties(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES))
                .addSchema(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        // When
        final CloseableIterable<? extends Element> elements = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(blankUser.getUserId())
                        .opAuth(ALL_USERS)
                        .build()));

        // Then
        assertFalse(elements.iterator().hasNext());

        try {
            store.execute(new GetAllElements(),
                    new Context(new User.Builder()
                            .userId(blankUser.getUserId())
                            .opAuths("x")
                            .build()));
            fail("expected exception");
        } catch (final OperationException e) {
            assertEquals(NO_RESULTS_TO_MERGE_ERROR, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldReturnSpecificGraphsFromCSVString() throws Exception {
        // Given

        final List<Collection<Graph>> graphLists = populateGraphs(1, 2, 4);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "mockGraphId1,mockGraphId2,mockGraphId4");

        // Then
        assertTrue(returnedGraphs.size() == 3);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    @Test
    public void shouldReturnNoGraphsFromEmptyString() throws Exception {
        // Given

        final List<Collection<Graph>> graphLists = populateGraphs();
        final Collection<Graph> expectedGraphs = graphLists.get(0);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "");

        // Then
        assertTrue(returnedGraphs.isEmpty());
        assertTrue(expectedGraphs.isEmpty());
    }

    @Test
    public void shouldReturnGraphsWithLeadingCommaString() throws Exception {
        // Given
        final List<Collection<Graph>> graphLists = populateGraphs(2, 4);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, ",mockGraphId2,mockGraphId4");

        // Then
        assertTrue(returnedGraphs.size() == 2);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    @Test
    public void shouldAddGraphIdWithAuths() throws Exception {
        // Given
        final Graph fedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(FEDERATED_STORE_ID)
                        .library(library)
                        .build())
                .addStoreProperties(federatedProperties)
                .build();

        addGraphWithIds(MAP_ID_1, ID_PROPS_MAP, ID_SCHEMA_ENTITY);

        library.add(MAP_ID_1, getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON), getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES));

        // When
        int before = 0;
        for (String ignore : fedGraph.execute(
                new GetAllGraphIds(),
                blankUser)) {
            before++;
        }

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphAuths("auth")
                        .graphId(MAP_ID_1)
                        .build(),
                blankUser);


        int after = 0;
        for (String ignore : fedGraph.execute(
                new GetAllGraphIds(),
                blankUser)) {
            after++;
        }


        fedGraph.execute(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group("BasicEntity")
                                .vertex("v1")
                                .build())
                        .build(),
                blankUser);

        final CloseableIterable<? extends Element> elements = fedGraph.execute(
                new GetAllElements(),
                new User.Builder()
                        .userId(USER_ID + "Other")
                        .opAuth("auth")
                        .build());

        try {
            fedGraph.execute(
                    new GetAllElements(),
                    new User.Builder()
                            .userId(USER_ID + "Other")
                            .opAuths("x")
                            .build());
            fail("expected exception");
        } catch (final OperationException e) {
            assertEquals(NO_RESULTS_TO_MERGE_ERROR, e.getCause().getMessage());
        }

        // Then
        assertEquals(0, before);
        assertEquals(1, after);
        assertNotNull(elements);
        assertTrue(elements.iterator().hasNext());
    }

    @Test
    public void shouldThrowWithPropertiesErrorFromGraphLibrary() throws Exception {
        Builder schema = new Builder();
        for (String path : new String[]{PATH_BASIC_EDGE_SCHEMA_JSON}) {
            schema.merge(getSchemaFromPath(path));
        }
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "test Something went wrong";
        Mockito.when(mockLibrary.getProperties(ID_PROPS_MAP)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);
        clearCache();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        try {
            store.execute(new AddGraph.Builder()
                    .graphId(MAP_ID_1)
                    .parentPropertiesId(ID_PROPS_MAP)
                    .isPublic(true)
                    .schema(schema.build())
                    .build(), userContext);

            fail("exception not thrown");
        } catch (Exception e) {
            assertEquals(error, e.getCause().getMessage());
        }

        Mockito.verify(mockLibrary).getProperties(ID_PROPS_MAP);
    }

    @Test
    public void shouldThrowWithSchemaErrorFromGraphLibrary() throws Exception {
        // Given
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "test Something went wrong";
        Mockito.when(mockLibrary.getSchema(ID_SCHEMA_ENTITY)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);
        clearCache();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        // When / Then
        try {
            store.execute(new AddGraph.Builder()
                    .graphId(MAP_ID_1)
                    .storeProperties(getPropertiesFromPath(PATH_MAP_STORE_PROPERTIES))
                    .isPublic(true)
                    .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_ENTITY))
                    .build(), userContext);
            fail(EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            assertEquals(error, e.getCause().getMessage());
        }
        Mockito.verify(mockLibrary).getSchema(ID_SCHEMA_ENTITY);
    }

    @Test
    public void shouldReturnASingleGraph() throws Exception {
        // Given
        final List<Collection<Graph>> graphLists = populateGraphs(1);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "mockGraphId1");

        // Then
        assertTrue(returnedGraphs.size() == 1);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    @Test
    public void shouldThrowExceptionWithInvalidCacheClass() throws StoreException {
        federatedProperties.setCacheProperties(INVALID_CACHE_SERVICE_CLASS_STRING);
        try {
            clearCache();
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Failed to instantiate cache"));
        }
    }

    @Test
    public void shouldReuseGraphsAlreadyInCache() throws Exception {
        //Check cache is empty
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        assertNull(CacheServiceLoader.getService());

        //initialise FedStore
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //add something so it will be in the cache
        Graph graphToAdd = new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        store.addGraphs(null, TEST_USER, true, graphToAdd);

        //check the store and the cache
        assertEquals(1, store.getAllGraphIds(blankUser).size());
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(MAP_ID_1));
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(MAP_ID_1));

        //restart the store
        store = new FederatedStore();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //check the graph is already in there from the cache
        assertTrue("Keys: " + CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME) + " did not contain " + MAP_ID_1, CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(MAP_ID_1));
        assertEquals(1, store.getAllGraphIds(blankUser).size());
    }

    @Test
    public void shouldInitialiseWithCache() throws StoreException {
        assertNull(CacheServiceLoader.getService());
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        assertNull(CacheServiceLoader.getService());
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        assertNotNull(CacheServiceLoader.getService());
    }

    @Test
    public void shouldThrowExceptionWithoutInitialisation() throws StoreException {
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Given
        Graph graphToAdd = new Graph.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        clearCache();

        // When / Then
        try {
            store.addGraphs(null, TEST_USER, false, graphToAdd);
            fail(EXCEPTION_NOT_THROWN);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("No cache has been set"));
        }
    }

    @Test
    public void shouldNotThrowExceptionWhenInitialisedWithNoCacheClassInProperties() throws StoreException {
        // Given
        federatedProperties = new FederatedStoreProperties();

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        } catch (final StoreException e) {
            fail("FederatedStore does not have to have a cache.");
        }
    }

    @Test
    public void shouldAddGraphsToCache() throws Exception {
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Given
        Graph graphToAdd = new Graph.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        // When
        store.addGraphs(null, TEST_USER, true, graphToAdd);

        // Then
        assertEquals(1, store.getGraphs(blankUser, ACC_ID_1).size());

        // When
        Collection<Graph> storeGraphs = store.getGraphs(blankUser, null);

        // Then
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1));
        assertTrue(storeGraphs.contains(graphToAdd));

        // When
        store = new FederatedStore();


        // Then
        assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1));
    }

    @Test
    public void shouldAddMultipleGraphsToCache() throws Exception {
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        // Given

        List<Graph> graphsToAdd = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            graphsToAdd.add(new Graph.Builder()
                    .config(new GraphConfig(ACC_ID_1 + i))
                    .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                    .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                    .build());
        }

        // When
        store.addGraphs(null, TEST_USER, false, graphsToAdd.toArray(new Graph[graphsToAdd.size()]));

        // Then
        for (int i = 0; i < 10; i++) {
            assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1 + i));
        }

        // When
        store = new FederatedStore();


        // Then
        for (int i = 0; i < 10; i++) {
            assertTrue(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME).contains(ACC_ID_1 + i));
        }
    }

    @Test
    public void shouldAddAGraphRemoveAGraphAndBeAbleToReuseTheGraphId() throws Exception {
        // Given
        // When
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.execute(new RemoveGraph.Builder()
                .graphId(MAP_ID_1)
                .build(), userContext);
        addGraphWithPaths(MAP_ID_1, PATH_MAP_STORE_PROPERTIES, PATH_BASIC_EDGE_SCHEMA_JSON);

        // Then
        final Collection<Graph> graphs = store.getGraphs(userContext.getUser(), MAP_ID_1);
        assertEquals(1, graphs.size());
        JsonAssert.assertEquals(
                JSONSerialiser.serialise(Schema.fromJson(StreamUtil.openStream(getClass(), PATH_BASIC_EDGE_SCHEMA_JSON))),
                JSONSerialiser.serialise(graphs.iterator().next().getSchema())
        );
    }

    private boolean checkUnexpected(final Collection<Graph> unexpectedGraphs, final Collection<Graph> returnedGraphs) {
        for (Graph graph : unexpectedGraphs) {
            if (returnedGraphs.contains(graph)) {
                return true;
            }
        }
        return false;
    }

    private List<Collection<Graph>> populateGraphs(int... expectedIds) throws Exception {
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
            store.addGraphs(Sets.newHashSet(ALL_USERS), null, true, tempGraph);
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
                        .build(), new Context(blankUser));

        return (null == elements) ? Sets.newHashSet() : Sets.newHashSet(elements);
    }

    private void assertContains(final Throwable e, final String format, final String... s) {
        final String expectedStr = String.format(format, s);
        boolean contains = e.getMessage().contains(expectedStr);
        assertTrue("\"" + e.getMessage() + "\" does not contain string \"" + expectedStr + "\"", contains);
    }

    private void addGraphWithIds(final String graphId, final String propertiesId, final String... schemaId) throws OperationException {
        ArrayList<String> schemas = Lists.newArrayList(schemaId);
        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .parentPropertiesId(propertiesId)
                .isPublic(true)
                .parentSchemaIds(schemas)
                .build(), userContext);
    }

    private void addGraphWithPaths(final String graphId, final String propertiesPath, final String... schemaPath) throws OperationException {
        Schema.Builder schema = new Builder();
        for (String path : schemaPath) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .storeProperties(getPropertiesFromPath(propertiesPath))
                .isPublic(true)
                .schema(schema.build())
                .build(), userContext);
    }

    private StoreProperties getPropertiesFromPath(final String pathMapStoreProperties) {
        return StoreProperties.loadStoreProperties(pathMapStoreProperties);
    }

    private Schema getSchemaFromPath(final String path) {
        return Schema.fromJson(StreamUtil.openStream(Schema.class, path));
    }

    private void clearCache() {
        CacheServiceLoader.shutdown();
    }

    private void clearLibrary() {
        HashMapGraphLibrary.clear();
    }
}
