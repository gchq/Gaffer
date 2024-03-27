/*
 * Copyright 2024 Crown Copyright
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

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.ChangeGraphId;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;

public class FederatedChangeGraphIdHandlerTest {
    private static final AccumuloProperties ACCUMULO_STORE_PROPERTIES = FederatedStoreTestUtil.loadAccumuloStoreProperties("properties/accumuloStore.properties");
    private static final FederatedStoreProperties FEDERATED_STORE_PROPERTIES = FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache();
    private static final Edge EDGE = FederatedStoreTestUtil.edgeBasic();
    private static final String INITIAL_GRAPH_ID = "basicGraph1";
    private static final String CHANGED_GRAPH_ID = "basicGraph2";

    private FederatedChangeGraphIdHandler handler;
    private FederatedStore federatedStore;

    @BeforeEach
    public void setup() throws StoreException {
        FederatedStoreTestUtil.resetForFederatedTests();

        handler = new FederatedChangeGraphIdHandler();

        federatedStore = new FederatedStore();
        federatedStore.initialise("testFedStore", null, FEDERATED_STORE_PROPERTIES);
    }

    @AfterEach
    public void after() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldChangeGraphId() throws OperationException {
        // Run tests with standard Accumulo Store
        testChangeGraphId(ACCUMULO_STORE_PROPERTIES);
    }

    @Test
    public void shouldChangeGraphIdWithNamespaces() throws OperationException {
        // Create an Accumulo Properties with Namespace property
        AccumuloProperties propsWithNamespaces = ACCUMULO_STORE_PROPERTIES.clone();
        propsWithNamespaces.setNamespace("testNamespace");

        // Run tests with Accumulo Store using Namespaces
        testChangeGraphId(propsWithNamespaces);
    }

    private void testChangeGraphId(AccumuloProperties accumuloProperties) throws OperationException {
        // Given

        // Add Graph
        AddGraph addGraph1 = new AddGraph.Builder()
            .graphId(INITIAL_GRAPH_ID)
            .schema(loadSchemaFromJson(SCHEMA_EDGE_BASIC_JSON))
            .storeProperties(accumuloProperties)
            .build();
        federatedStore.execute(addGraph1, contextTestUser());
        // Add an Edge
        federatedStore.execute(new AddElements.Builder().input(EDGE).build(), contextTestUser());

        // When

        // Check there is a subgraph with the correct id
        Iterable<String> graphs = (Iterable<String>) federatedStore.execute(new GetAllGraphIds(), contextTestUser());
        assertThat(graphs).containsOnly(INITIAL_GRAPH_ID);
        // Check the edge exists
        Iterable<Element> getAllEle1 = (Iterable<Element>) federatedStore.execute(new GetAllElements(), contextTestUser());
        assertThat(getAllEle1).containsExactly(EDGE);
        // Change the graph id
        ChangeGraphId changeGraphIdOp = new ChangeGraphId.Builder().graphId(INITIAL_GRAPH_ID).newGraphId(CHANGED_GRAPH_ID).build();
        boolean result = handler.doOperation(changeGraphIdOp, contextTestUser(), federatedStore);
        assertThat(result).isTrue();

        // Then

        // Check the subgraph id has been renamed
        graphs = (Iterable<String>) federatedStore.execute(new GetAllGraphIds(), contextTestUser());
        assertThat(graphs).containsOnly(CHANGED_GRAPH_ID);
        // Check the edge still exists
        Iterable<Element> getAllEle2 = (Iterable<Element>) federatedStore.execute(new GetAllElements(), contextTestUser());
        assertThat(getAllEle2).containsExactly(EDGE);
        // Check a new graph using the original id can be added
        federatedStore.execute(addGraph1, contextTestUser());
        graphs = (Iterable<String>) federatedStore.execute(new GetAllGraphIds(), contextTestUser());
        assertThat(graphs).containsExactly(CHANGED_GRAPH_ID, INITIAL_GRAPH_ID);
    }
}

