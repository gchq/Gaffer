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
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S;

public class FederatedStoreTest {
    public static final String PATH_FEDERATED_STORE_PROPERTIES = "/properties/federatedStoreTest.properties";
    public static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    public static final String ACC_ID_1 = "mockAccGraphId1";
    public static final String MAP_ID_1 = "mockMapGraphId1";
    public static final String PATH_ACC_STORE_PROPERTIES = "properties/singleUseMockAccStore.properties";
    public static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMockMapStore.properties";
    public static final String PATH_MAP_STORE_PROPERTIES_ALT = "properties/singleUseMockMapStoreAlt.properties";
    public static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    public static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    public static final String KEY_ACC_ID1_PROPERTIES_FILE = "gaffer.federatedstore.mockAccGraphId1.properties.file";
    public static final String KEY_MAP_ID1_PROPERTIES_FILE = "gaffer.federatedstore.mockMapGraphId1.properties.file";
    public static final String KEY_MAP_ID1_PROPERTIES_ID = "gaffer.federatedstore.mockMapGraphId1.properties.id";
    public static final String GRAPH_IDS = "gaffer.federatedstore.graphIds";
    public static final String KEY_ACC_ID1_SCHEMA_FILE = "gaffer.federatedstore.mockAccGraphId1.schema.file";
    public static final String KEY_MAP_ID1_SCHEMA_FILE = "gaffer.federatedstore.mockMapGraphId1.schema.file";
    public static final String KEY_MAP_ID1_SCHEMA_ID = "gaffer.federatedstore.mockMapGraphId1.schema.id";
    public static final String PATH_INVALID = "nothing.json";
    public static final String EXCEPTION_NOT_THROWN = "exception not thrown";
    public static final String USER_ID = "testUser";
    public static final User TEST_USER = new User.Builder().userId(USER_ID).opAuths("one", "two").build();
    public static final String PROPS_ID_1 = "PROPS_ID_1";
    public static final String SCHEMA_ID_1 = "SCHEMA_ID_1";
    FederatedStore store;
    private StoreProperties federatedProperties;

    @Before
    public void setUp() throws Exception {
        store = new FederatedStore();
        federatedProperties = new StoreProperties();
        HashMapGraphLibrary.clear();
    }

    @Test
    public void shouldLoadGraphsWithIds() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, ACC_ID_1 + "," + MAP_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);

        //Then
        int before = store.getGraphs(null).size();

        //When
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //Then
        Collection<Graph> graphs = store.getGraphs(null);
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
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_INVALID);

        //When
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        } catch (final IllegalArgumentException e) {
            //Then
            assertEquals(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Schema", "graphId: " + MAP_ID_1 + " schemaPath: " + PATH_INVALID), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldThrowErrorForFailedProperty() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_INVALID);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);

        //When
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        } catch (final IllegalArgumentException e) {
//            Then
            assertEquals(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Property", "graphId: " + MAP_ID_1 + " propertyPath: " + PATH_INVALID), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldThrowErrorForIncompleteBuilder() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);

        //When
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        } catch (final IllegalArgumentException e) {
            //Then
            assertTrue(e.getMessage().contains(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", "")));
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldNotAllowOverwritingOfGraphWithFederatedScope() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        try {
            store.addGraphs(new Graph.Builder()
                    .config(new GraphConfig(ACC_ID_1))
                    .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_ACC_STORE_PROPERTIES))
                    .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                    .build());
        } catch (final Exception e) {
            assertEquals(String.format(USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S, ACC_ID_1), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldDoUnhandledOperation() throws Exception {
        store.doUnhandledOperation(null, null);
    }

    @Test
    public void shouldUpdateTraitsWhenNewGraphIsAdded() throws Exception {
        federatedProperties.set(GRAPH_IDS, ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //With less Traits
        Set<StoreTrait> before = store.getTraits();

        store.addGraphs(new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        //includes same as before but with more Traits
        Set<StoreTrait> after = store.getTraits();
        assertEquals("Sole graph has 9 traits, so all traits of the federatedStore is 9", 9, before.size());
        assertEquals("the two graphs share 5 traits", 5, after.size());
        assertNotEquals(before, after);
        assertTrue(before.size() > after.size());
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsAdded() throws Exception {
        federatedProperties.set(GRAPH_IDS, ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        Schema before = store.getSchema();

        store.addGraphs(new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build());

        Schema after = store.getSchema();
        assertNotEquals(before, after);
    }

    @Test
    public void shouldUpdateTraitsToMinWhenGraphIsRemoved() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1 + "," + ACC_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //With less Traits
        Set<StoreTrait> before = store.getTraits();
        store.remove(MAP_ID_1);
        Set<StoreTrait> after = store.getTraits();

        //includes same as before but with more Traits
        assertEquals("Shared traits between the two graphs should be " + 5, 5, before.size());
        assertEquals("Shared traits counter-intuitively will go up after removing graph, because the sole remaining graph has 9 traits", 9, after.size());
        assertNotEquals(before, after);
        assertTrue(before.size() < after.size());
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsRemoved() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1 + "," + ACC_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        Schema before = store.getSchema();

        store.remove(MAP_ID_1);

        Schema after = store.getSchema();
        assertNotEquals(before, after);
    }

    @Test
    public void shouldFailWithIncompleteSchema() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, "/schema/edgeX2NoTypesSchema.json");


        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains(String.format(S1_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_SUPPLIED_PROPERTIES_GRAPH_ID_S2, "Graph", "")));
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldTakeCompleteSchemaFromTwoFiles() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, "/schema/edgeX2NoTypesSchema.json" + ", /schema/edgeTypeSchema.json");


        int before = store.getGraphs(null).size();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        int after = store.getGraphs(null).size();

        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        federatedProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        // When
        int sizeBefore = store.getGraphs(null).size();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        int sizeAfter = store.getGraphs(null).size();

        //Then
        assertEquals(0, sizeBefore);
        assertEquals(2, sizeAfter);
    }

    @Test
    public void shouldCombineTraitsToMin() throws Exception {
        federatedProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        //Given
        HashSet<StoreTrait> traits = new HashSet<>();
        traits.addAll(SingleUseAccumuloStore.TRAITS);
        traits.retainAll(MapStore.TRAITS);

        //When
        Set<StoreTrait> before = store.getTraits();
        int sizeBefore = before.size();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Set<StoreTrait> after = store.getTraits();
        int sizeAfter = after.size();

        //Then
        assertEquals(5, MapStore.TRAITS.size());
        assertEquals(9, SingleUseAccumuloStore.TRAITS.size());
        assertNotEquals(SingleUseAccumuloStore.TRAITS, MapStore.TRAITS);
        assertEquals(0, sizeBefore);
        assertEquals(5, sizeAfter);
        assertEquals(traits, after);
    }

    @Test
    public void shouldContainNoElements() throws Exception {
        federatedProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        Set<Element> after = getElements();
        assertEquals(0, after.size());
    }

    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        federatedProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        AddElements op = new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("BasicEdge")
                        .source("testSource")
                        .dest("testDest")
                        .property("property1", 12)
                        .build())
                .build();

        store.execute(op, new Context(TEST_USER));

        assertEquals(1, getElements().size());
    }

    @Test
    public void shouldReturnGraphIds() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1 + "," + ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        Set<String> allGraphIds = store.getAllGraphIds();

        assertEquals(2, allGraphIds.size());
        assertTrue(allGraphIds.contains(ACC_ID_1));
        assertTrue(allGraphIds.contains(MAP_ID_1));

    }

    @Test
    public void shouldUpdateGraphIds() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        Set<String> allGraphId = store.getAllGraphIds();

        assertEquals(1, allGraphId.size());
        assertTrue(allGraphId.contains(ACC_ID_1));
        assertFalse(allGraphId.contains(MAP_ID_1));

        store.addGraphs(new Graph.Builder()
                .config(new GraphConfig(MAP_ID_1))
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());


        Set<String> allGraphId2 = store.getAllGraphIds();

        assertEquals(2, allGraphId2.size());
        assertTrue(allGraphId2.contains(ACC_ID_1));
        assertTrue(allGraphId2.contains(MAP_ID_1));

        store.remove(ACC_ID_1);

        Set<String> allGraphId3 = store.getAllGraphIds();

        assertEquals(1, allGraphId3.size());
        assertFalse(allGraphId3.contains(ACC_ID_1));
        assertTrue(allGraphId3.contains(MAP_ID_1));

    }

    private Set<Element> getElements() throws uk.gov.gchq.gaffer.operation.OperationException {
        CloseableIterable<? extends Element> elements = store
                .execute(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema().getEdgeGroups())
                                .entities(store.getSchema().getEntityGroups())
                                .build())
                        .build(), new Context(TEST_USER));

        return (null == elements) ? Sets.newHashSet() : Sets.newHashSet(elements);
    }

    @Test
    public void shouldGetAllGraphIdsInUnmodifiableSet() throws Exception {
        // Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_ID, PROPS_ID_1);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenReturn(StoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES));
        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When / Then
        try {
            store.getAllGraphIds().add("newId");
            fail("Exception expected");
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotUseSchema() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final Schema unusedMock = Mockito.mock(Schema.class);
        Mockito.verifyNoMoreInteractions(unusedMock);

        store.initialise(FEDERATED_STORE_ID, unusedMock, federatedProperties);
    }

    @Test
    public void shouldAddGraphFromLibrary() throws Exception {
        //Given
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.exists(MAP_ID_1)).thenReturn(true);
        Mockito.when(mockLibrary.get(MAP_ID_1)).thenReturn(
                new Pair<>(
                        new Schema.Builder()
                                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                                .build(),
                        StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES))
                ));

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //When
        final int before = store.getGraphs(null).size();
        store.execute(new AddGraph.Builder()
                .graphId(MAP_ID_1)
                .build(), new Context(new User.Builder()
                .userId(USER_ID)
                .build()));

        final int after = store.getGraphs(null).size();
        //Then
        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddNamedGraphFromGraphIDKeyButDefinedInLibrary() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.exists(MAP_ID_1)).thenReturn(true);
        Mockito.when(mockLibrary.get(MAP_ID_1)).thenReturn(
                new Pair<>(
                        new Schema.Builder()
                                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                                .build(),
                        StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES))
                ));

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //Then
        final int after = store.getGraphs(null).size();
        assertEquals(1, after);
    }

    @Test
    public void shouldAddGraphFromGraphIDKeyButDefinedProperties() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);

        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //Then
        final int after = store.getGraphs(null).size();
        assertEquals(1, after);
    }

    @Test
    public void shouldAddNamedGraphsFromGraphIDKeyButDefinedInLibraryAndProperties() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1 + ", " + ACC_ID_1);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES_FILE, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA_FILE, PATH_BASIC_ENTITY_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.exists(MAP_ID_1)).thenReturn(true);
        Mockito.when(mockLibrary.get(MAP_ID_1)).thenReturn(
                new Pair<>(
                        new Schema.Builder()
                                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                                .build(),
                        StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES))
                ));

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //Then
        final int after = store.getGraphs(null).size();
        assertEquals(2, after);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibrary() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_ID, PROPS_ID_1);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenReturn(StoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES));

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        assertEquals(1, store.getGraphs(null).size());

        Mockito.verify(mockLibrary).getProperties(PROPS_ID_1);
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibrary() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_ID, SCHEMA_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1))
                .thenReturn(new Schema.Builder()
                        .json(StreamUtil.openStream(FederatedStore.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                        .build());

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        assertEquals(1, store.getGraphs(null).size());

        Mockito.verify(mockLibrary).getSchema(SCHEMA_ID_1);
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibrary() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_ID, PROPS_ID_1);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_ID, SCHEMA_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1))
                .thenReturn(StoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES));
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1))
                .thenReturn(new Schema.Builder()
                        .json(StreamUtil.openStream(FederatedStore.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                        .build());

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        assertEquals(1, store.getGraphs(null).size());

        Mockito.verify(mockLibrary).getSchema(SCHEMA_ID_1);
        Mockito.verify(mockLibrary).getProperties(PROPS_ID_1);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibraryOverridden() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_ID, PROPS_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final MapStoreProperties prop = new MapStoreProperties();
        final String unusualKey = "unusualKey";
        prop.set(unusualKey, "value");
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenReturn(prop);

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        assertEquals(1, store.getGraphs(null).size());
        assertTrue(store.getGraphs(null).iterator().next().getStoreProperties().getProperties().getProperty(unusualKey) != null);

        Mockito.verify(mockLibrary).getProperties(PROPS_ID_1);
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibraryOverridden() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_ID, SCHEMA_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1)).thenReturn(new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        assertEquals(1, store.getGraphs(null).size());
        assertTrue(store.getGraphs(null).iterator().next().getSchema().getEntityGroups().contains("BasicEntity"));

        Mockito.verify(mockLibrary).getSchema(SCHEMA_ID_1);
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibraryOverridden() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_ID, PROPS_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_ID, SCHEMA_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final MapStoreProperties prop = new MapStoreProperties();
        final String unusualKey = "unusualKey";
        prop.set(unusualKey, "value");
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenReturn(prop);
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1)).thenReturn(new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        assertEquals(1, store.getGraphs(null).size());
        assertTrue(store.getGraphs(null).iterator().next().getStoreProperties().getProperties().getProperty(unusualKey) != null);
        assertTrue(store.getGraphs(null).iterator().next().getSchema().getEntityGroups().contains("BasicEntity"));

        Mockito.verify(mockLibrary).getProperties(PROPS_ID_1);
        Mockito.verify(mockLibrary).getSchema(SCHEMA_ID_1);
    }

    @Test
    public void should() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        Mockito.when(mockLibrary.get(MAP_ID_1)).thenReturn(
                new Pair<>(
                        new Schema.Builder()
                                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_EDGE_SCHEMA_JSON))
                                .build(),
                        StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES))
                ));
        final MapStoreProperties prop = new MapStoreProperties();
        final String unusualKey = "unusualKey";
        prop.set(unusualKey, "value");
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenReturn(prop);
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1)).thenReturn(new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        store.setGraphLibrary(mockLibrary);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        //No exception to be thrown.
    }

    @Test
    public void shouldNotAllowOverridingOfKnownGraphInLibrary() throws Exception {
        //Given
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES_ALT);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final Schema schema = new Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES));
        final GraphLibrary lib = new HashMapGraphLibrary();
        lib.add(MAP_ID_1, schema, storeProperties);

        try {
            store.setGraphLibrary(lib);
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail("exception should have been thrown");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getCause().getMessage().contains("GraphId " + MAP_ID_1 + " already exists with a different store properties:"));
        }
    }

    @Test
    public void shouldFederatedIfUserHasCorrectAuths() throws Exception {
        //Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        store.addGraphs(new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(MAP_ID_1)
                        .addHook(new FederatedAccessHook.Builder()
                                .graphAuths("auth1")
                                .build())
                        .build())
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        final CloseableIterable<? extends Element> elements = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(USER_ID)
                        .opAuth("auth1")
                        .build()));

        Assert.assertFalse(elements.iterator().hasNext());

        final CloseableIterable<? extends Element> x = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(USER_ID)
                        .opAuths("x")
                        .build()));

        Assert.assertNull(x);
    }

    @Test
    public void shouldReturnSpecificGraphsFromCSVString() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        final List<Collection<Graph>> graphLists = populateGraphs(1, 2, 4);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs("mockGraphId1,mockGraphId2,mockGraphId4");

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
        final Collection<Graph> returnedGraphs = store.getGraphs("");

        // Then
        assertTrue(returnedGraphs.isEmpty());
        assertEquals(expectedGraphs, returnedGraphs);
    }

    @Test
    public void shouldReturnGraphsWithLeadingCommaString() throws StoreException {
        // Given
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        final List<Collection<Graph>> graphLists = populateGraphs(2, 4);
        final Collection<Graph> expectedGraphs = graphLists.get(0);
        final Collection<Graph> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(",mockGraphId2,mockGraphId4");

        // Then
        assertTrue(returnedGraphs.size() == 2);
        assertTrue(returnedGraphs.containsAll(expectedGraphs));
        assertFalse(checkUnexpected(unexpectedGraphs, returnedGraphs));
    }

    @Test
    public void shouldAddGraphIdWithAuths() throws Exception {
        federatedProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");
        federatedProperties.set("gaffer.store.properties.class", "uk.gov.gchq.gaffer.store.StoreProperties");
        federatedProperties.set(FederatedStoreConstants.SKIP_FAILED_FEDERATED_STORE_EXECUTE, "false");

        //Given
        final HashMapGraphLibrary hashMapGraphLibrary = new HashMapGraphLibrary();
        hashMapGraphLibrary.add(MAP_ID_1, new Schema.Builder()
                .json(StreamUtil.openStream(this.getClass(), PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build(), StoreProperties.loadStoreProperties(StreamUtil.openStream(this.getClass(), PATH_MAP_STORE_PROPERTIES)));

        final Graph store = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(FEDERATED_STORE_ID)
                        .library(hashMapGraphLibrary)
                        .build())
                .addStoreProperties(federatedProperties)
                .build();

        //When
        int before = 0;
        for (String ignore : store.execute(
                new GetAllGraphIds(),
                new User.Builder()
                        .userId(USER_ID)
                        .build())) {
            before++;
        }

        store.execute(
                new AddGraph.Builder()
                        .graphAuths("auth")
                        .graphId(MAP_ID_1)
                        .build(),
                new User.Builder()
                        .userId(USER_ID)
                        .build());


        int after = 0;
        for (String ignore : store.execute(
                new GetAllGraphIds(),
                new User.Builder()
                        .userId(USER_ID)
                        .build())) {
            after++;
        }


        store.execute(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group("BasicEntity")
                                .vertex("hi")
                                .build())
                        .build(),
                new User.Builder()
                        .userId(USER_ID)
                        .opAuth("auth")
                        .build());

        final CloseableIterable<? extends Element> elements = store.execute(
                new GetAllElements(),
                new User.Builder()
                        .userId(USER_ID + "Other")
                        .opAuth("auth")
                        .build());

        final CloseableIterable<? extends Element> x = store.execute(
                new GetAllElements(),
                new User.Builder()
                        .userId(USER_ID + "Other")
                        .opAuths("x")
                        .build());

        //Then
        assertEquals(0, before);
        assertEquals(1, after);
        Assert.assertNotNull(elements);
        Assert.assertTrue(elements.iterator().hasNext());
        Assert.assertNull(x);
    }

    @Test
    public void shouldThrowWithPropertiesErrorFromGraphLibrary() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_ID, PROPS_ID_1);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_FILE, PATH_BASIC_EDGE_SCHEMA_JSON);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "Something went wrong";
        Mockito.when(mockLibrary.getProperties(PROPS_ID_1)).thenThrow(new IllegalArgumentException(error));

        store.setGraphLibrary(mockLibrary);

        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail("exception not thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(error, e.getCause().getMessage());
        }

        Mockito.verify(mockLibrary).getProperties(PROPS_ID_1);
    }

    @Test
    public void shouldThrowWithSchemaErrorFromGraphLibrary() throws Exception {
        federatedProperties.set(GRAPH_IDS, MAP_ID_1);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES_FILE, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA_ID, SCHEMA_ID_1);
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "Something went wrong";
        Mockito.when(mockLibrary.getSchema(SCHEMA_ID_1)).thenThrow(new IllegalArgumentException(error));

        store.setGraphLibrary(mockLibrary);

        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
            fail("exception not thrown");
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
        final Collection<Graph> returnedGraphs = store.getGraphs("mockGraphId1");

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

    private List<Collection<Graph>> populateGraphs(int... expectedIds) {
        final Collection<Graph> expectedGraphs = new ArrayList<>();
        final Collection<Graph> unexpectedGraphs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Graph tempGraph = new Graph.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId("mockGraphId" + i)
                            .addHook(new FederatedAccessHook.Builder()
                                    .graphAuths("auth" + i)
                                    .build())
                            .build())
                    .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                    .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                    .build();
            store.addGraphs(tempGraph);
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
}
