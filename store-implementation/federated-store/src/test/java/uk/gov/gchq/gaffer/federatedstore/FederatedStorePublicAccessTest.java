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

package uk.gov.gchq.gaffer.federatedstore;

import org.assertj.core.api.IterableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.util.Properties;

import static java.util.Objects.isNull;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStorePublicAccessTest {

    private static final Context BLANK_USER_CONTEXT = contextBlankUser();
    private static final Context TEST_USER_CONTEXT = contextTestUser();
    private static final AccumuloProperties PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private FederatedStore store;
    private HashMapGraphLibrary library;
    private Graph federatedGraph;

    private static void getAllGraphsIdsIsEmpty(FederatedStore store, final boolean isEmpty) throws uk.gov.gchq.gaffer.operation.OperationException {
        Iterable<String> results = (Iterable<String>) store.execute(new GetAllGraphIds(), BLANK_USER_CONTEXT);

        final IterableAssert<String> anAssert = assertThat(results).isNotNull();
        if (isEmpty) {
            anAssert.isEmpty();
        } else {
            anAssert.isNotEmpty()
                    .containsExactly(GRAPH_ID_ACCUMULO);
        }
    }

    public static FederatedStoreProperties createProperties() {
        FederatedStoreProperties fedProps  = new FederatedStoreProperties();
        try {
            Properties props = new Properties();
            props.load(FederatedStoreVisibilityTest.class.getResourceAsStream("/properties/federatedStore.properties"));
            fedProps.setProperties(props);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fedProps .setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        return fedProps;
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties props = createProperties();

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(props)
                .build();

        store = new FederatedStore();
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsDefaultedPrivateAndGraphIsDefaultedPrivate() throws Exception {
        FederatedStoreProperties props = createProperties();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(null), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, true);
    }

    @Test
    public void shouldBePublicWhenAllGraphsDefaultedPrivateAndGraphIsSetPublic() throws Exception {
        FederatedStoreProperties props = createProperties();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(true), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, false);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsDefaultedPrivateAndGraphIsSetPrivate() throws Exception {
        FederatedStoreProperties props = createProperties();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(false), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, true);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPrivateAndGraphIsSetPublic() throws Exception {
        FederatedStoreProperties props = createProperties();
        props.setFalseGraphsCanHavePublicAccess();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(true), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, true);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPrivateAndGraphIsSetPrivate() throws Exception {
        FederatedStoreProperties props = createProperties();
        props.setFalseGraphsCanHavePublicAccess();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(false), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, true);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPublicAndGraphIsSetPrivate() throws Exception {
        FederatedStoreProperties props = createProperties();
        props.setTrueGraphsCanHavePublicAccess();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(false), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, true);
    }

    @Test
    public void shouldBePublicWhenAllGraphsSetPublicAndGraphIsSetPublic() throws Exception {
        FederatedStoreProperties props = createProperties();
        props.setTrueGraphsCanHavePublicAccess();
        store.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);
        store.execute(addGraph(true), TEST_USER_CONTEXT);
        getAllGraphsIdsIsEmpty(store, false);
    }

    private AddGraph addGraph(final Boolean isPublic) {
        final AddGraph.Builder builder = new AddGraph.Builder()
                .graphId(GRAPH_ID_ACCUMULO)
                .storeProperties(PROPERTIES.clone())
                .schema(new Schema.Builder().build());

        return (isNull(isPublic)) ? builder.build() : builder.isPublic(isPublic).build();
    }

}
