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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.USER_IS_ATTEMPTING_TO_OVERWRITE_A_GRAPH_WITHIN_FEDERATED_STORE_GRAPH_ID_S;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.integration.FederatedStoreITs;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class FederatedStoreTest {
    public static final String PATH_FEDERATED_STORE_PROPERTIES = "/properties/federatedStoreTest.properties";
    public static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    public static final String ACC_ID_1 = "mockAccGraphId1";
    public static final String MAP_ID_1 = "mockMapGraphId1";
    public static final String PATH_ACC_STORE_PROPERTIES = "properties/singleUseMockAccStore.properties";
    public static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMockMapStore.properties";
    public static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    public static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    public static final String KEY_ACC_ID1_PROPERTIES = "gaffer.federatedstore.mockAccGraphId1.properties";
    public static final String KEY_MAP_ID1_PROPERTIES = "gaffer.federatedstore.mockMapGraphId1.properties";
    public static final String KEY_ACC_ID1_SCHEMA = "gaffer.federatedstore.mockAccGraphId1.schema";
    public static final String KEY_MAP_ID1_SCHEMA = "gaffer.federatedstore.mockMapGraphId1.schema";
    public static final String PATH_INVALID = "nothing.json";
    public static final String EXCEPTION_NOT_THROWN = "exception not thrown";
    public static final User TEST_USER = new User("testUser");
    FederatedStore store;
    private Schema mockSchema;
    private StoreProperties federatedProperties;

    @Before
    public void setUp() throws Exception {
        store = new FederatedStore();
        mockSchema = mock(Schema.class);
        federatedProperties = new StoreProperties();
    }

    @Test
    public void shouldLoadGraphsWithIds() throws Exception {
        //Given
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, PATH_BASIC_ENTITY_SCHEMA_JSON);
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA, PATH_BASIC_EDGE_SCHEMA_JSON);

        //Then
        int before = store.getGraphs().size();

        //When
        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);

        //Then
        Collection<Graph> graphs = store.getGraphs();
        int after = graphs.size();
        assertEquals(0, before);
        assertEquals(2, after);
        ArrayList<String> graphNames = Lists.newArrayList(ACC_ID_1, MAP_ID_1);
        for (Graph graph : graphs) {
            Assert.assertTrue(graphNames.contains(graph.getGraphId()));
        }
    }

    @Test
    public void shouldThrowErrorForFailedSchema() throws Exception {
        //Given
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA, PATH_INVALID);

        //When
        try {
            store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);
        } catch (final IllegalArgumentException e) {
            //Then
            assertEquals(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, MAP_ID_1), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldThrowErrorForFailedProperty() throws Exception {
        //Given
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES, PATH_INVALID);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA, PATH_BASIC_EDGE_SCHEMA_JSON);

        //When
        try {
            store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);
        } catch (final IllegalArgumentException e) {
//            Then
            assertEquals(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, MAP_ID_1), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldThrowErrorForIncompleteBuilder() throws Exception {
        //Given
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES, PATH_INVALID);

        //When
        try {
            store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);
        } catch (final IllegalArgumentException e) {
            //Then
            assertEquals(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, MAP_ID_1), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldNotAllowOverwritingOfGraph() throws Exception {
        //Given
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);

        try {
            store.add(new Graph.Builder()
                    .graphId(ACC_ID_1)
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
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);

        //With less Traits
        Set<StoreTrait> before = store.getTraits();

        store.add(new Graph.Builder()
                .graphId(MAP_ID_1)
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
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);

        Schema before = store.getSchema();

        store.add(new Graph.Builder()
                .graphId(MAP_ID_1)
                .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, PATH_MAP_STORE_PROPERTIES))
                .addSchema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build());

        Schema after = store.getSchema();
        assertNotEquals(before, after);
    }

    @Test
    public void shouldUpdateTraitsToMinWhenGraphIsRemoved() throws Exception {
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);

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
        federatedProperties.set(KEY_MAP_ID1_PROPERTIES, PATH_MAP_STORE_PROPERTIES);
        federatedProperties.set(KEY_MAP_ID1_SCHEMA, PATH_BASIC_EDGE_SCHEMA_JSON);
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, PATH_BASIC_ENTITY_SCHEMA_JSON);

        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);

        Schema before = store.getSchema();

        store.remove(MAP_ID_1);

        Schema after = store.getSchema();
        assertNotEquals(before, after);
    }

    @Test
    public void shouldFailWithIncompleteSchema() throws Exception {
        //Given
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, "/schema/edgeX2NoTypesSchema.json");


        try {
            store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);
        } catch (final Exception e) {
            assertEquals(String.format(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES_GRAPH_ID_S, ACC_ID_1), e.getMessage());
            return;
        }
        fail(EXCEPTION_NOT_THROWN);
    }

    @Test
    public void shouldTakeCompleteSchemaFromTwoFiles() throws Exception {
        //Given
        federatedProperties.set(KEY_ACC_ID1_PROPERTIES, PATH_ACC_STORE_PROPERTIES);
        federatedProperties.set(KEY_ACC_ID1_SCHEMA, "/schema/edgeX2NoTypesSchema.json" + ", /schema/edgeTypeSchema.json");


        int before = store.getGraphs().size();
        store.initialise(FEDERATED_STORE_ID, mockSchema, federatedProperties);
        int after = store.getGraphs().size();

        assertEquals(0, before);
        assertEquals(1, after);
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        federatedProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, PATH_FEDERATED_STORE_PROPERTIES));
        // When
        int sizeBefore = store.getGraphs().size();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        int sizeAfter = store.getGraphs().size();

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

        store.execute(op, TEST_USER);

        assertEquals(1, getElements().size());
    }

    private Set<Element> getElements() throws uk.gov.gchq.gaffer.operation.OperationException {
        CloseableIterable<? extends Element> elements = store
                .execute(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema().getEdgeGroups())
                                .entities(store.getSchema().getEntityGroups())
                                .build())
                        .build(), TEST_USER);

        return Sets.newHashSet(elements);
    }
}
