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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStore.GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class FederatedStoreTest {

    public static final String TEST_FEDERATED_STORE_ID = "TestFederatedStoreId";
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
        federatedProperties.set("gaffer.federatedstore.testGraphId1.properties", "testGraphId1.properties");
        federatedProperties.set("gaffer.federatedstore.testGraphId1.schema", "/path/to/schema1.json");
        federatedProperties.set("gaffer.federatedstore.testGraphId2.properties", "testGraphId2.properties");
        federatedProperties.set("gaffer.federatedstore.testGraphId2.schema", "/path/to/schema2.json, /path/to/schema2B.json, ");

        //Then
        Collection<Graph> graphs;
        graphs = store.getGraphs();
        assertEquals(0, graphs.size());

        //When
        store.initialise(TEST_FEDERATED_STORE_ID, mockSchema, federatedProperties);

        //Then
        graphs = store.getGraphs();
        assertEquals(2, graphs.size());
        ArrayList<String> graphNames = Lists.newArrayList("testGraphId1", "testGraphId2");
        for (Graph graph : graphs) {
            Assert.assertTrue(graphNames.contains(graph.getGraphId()));
        }
    }

    @Test
    public void shouldThrowErrorForFailedSchema() throws Exception {
        //Given
        federatedProperties.set("gaffer.federatedstore.testGraphId1.properties", "testGraphId1.properties");
        federatedProperties.set("gaffer.federatedstore.testGraphId1.schema", "/path/to/wrong.json");

        //When
        try {
            store.initialise(TEST_FEDERATED_STORE_ID, mockSchema, federatedProperties);
        } catch (final IllegalArgumentException e) {
            //Then
            assertEquals(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES, e.getMessage());
        }

    }

    @Test
    public void shouldThrowErrorForIncompleteBuilder() throws Exception {
        //Given
        federatedProperties.set("gaffer.federatedstore.testGraphId1.properties", "testGraphId1.properties");

        //When
        try {
            store.initialise(TEST_FEDERATED_STORE_ID, mockSchema, federatedProperties);
        } catch (final IllegalArgumentException e) {
            //Then
            assertEquals(GRAPH_WAS_NOT_ABLE_TO_BE_CREATED_WITH_THE_USER_SUPPLIED_PROPERTIES, e.getMessage());
        }
    }

    @Test
    public void shouldNotAllowOverwritingOfGraph() throws Exception {
        //Given
        federatedProperties.set("gaffer.federatedstore.testGraphId1.properties", "testGraphId1.properties");
        federatedProperties.set("gaffer.federatedstore.testGraphId1.schema", "/path/to/schema1.json");

        store.initialise(TEST_FEDERATED_STORE_ID, mockSchema, federatedProperties);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldDoUnhandledOperation() throws Exception {
        store.doUnhandledOperation(null, null);
    }

    @Test()
    public void shouldIsValidationRequired() throws Exception {
        assertFalse(store.isValidationRequired());
    }

    @Test
    public void shouldUpdateTraitsWhenNewGraphIsAdded() throws Exception {
        federatedProperties.set("gaffer.federatedstore.testGraphId1.properties", "testGraphId3.properties");
        federatedProperties.set("gaffer.federatedstore.testGraphId1.schema", "/path/to/schema1.json");

        store.initialise(TEST_FEDERATED_STORE_ID, mockSchema, federatedProperties);

        //With less Traits
        Set<StoreTrait> before = store.getTraits();

        store.add(new Graph.Builder()
                          .graphId("testGraph")
                          .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, "testGraphId1.properties"))
                          .addSchema(StreamUtil.openStream(FederatedStoreTest.class, "/path/to/schema2.json"))
                          .addSchema(StreamUtil.openStream(FederatedStoreTest.class, "/path/to/schema2b.json"))
                          .build());

        //includes same as before but with more Traits
        Set<StoreTrait> after = store.getTraits();
        assertNotEquals(before, after);
        assertTrue(before.size() < after.size());
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsAdded() throws Exception {
        federatedProperties.set("gaffer.federatedstore.testGraphId1.properties", "testGraphId3.properties");
        federatedProperties.set("gaffer.federatedstore.testGraphId1.schema", "/path/to/schema1.json");

        store.initialise(TEST_FEDERATED_STORE_ID, mockSchema, federatedProperties);

        Schema before = store.getSchema();

        store.add(new Graph.Builder()
                          .graphId("testGraphAlt")
                          .storeProperties(StreamUtil.openStream(FederatedStoreTest.class, "testGraphId1.properties"))
                          .addSchema(StreamUtil.openStream(FederatedStoreTest.class, "/path/to/schema2.json"))
                          .addSchema(StreamUtil.openStream(FederatedStoreTest.class, "/path/to/schema2b.json"))
                          .build());

        Schema after = store.getSchema();
        assertNotEquals(before, after);
    }
}