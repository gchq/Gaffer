/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.StoreUser;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FederatedStorePublicAccessTest {

    private static final String GRAPH_1 = "graph1";
    private static final String PROP_1 = "prop1";
    private static final String SCHEMA_1 = "schema1";
    private static final String TEST_FED_STORE_ID = "testFedStore";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final Context BLANK_USER_CONTEXT = new Context(StoreUser.blankUser());
    private static final Context TEST_USER_CONTEXT = new Context(StoreUser.testUser());
    private FederatedStore store;
    private FederatedStoreProperties fedProps;
    private HashMapGraphLibrary library;

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
        fedProps = new FederatedStoreProperties();
        fedProps.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        store = new FederatedStore();
        library = new HashMapGraphLibrary();
        HashMapGraphLibrary.clear();

        library.addProperties(PROP_1, PROPERTIES);
        library.addSchema(SCHEMA_1, new Schema.Builder().build());
        store.setGraphLibrary(library);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsDefaultedPrivateAndGraphIsDefaultedPrivate() throws Exception {
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(new AddGraph.Builder()
                .graphId(GRAPH_1)
                .parentPropertiesId(PROP_1)
                .parentSchemaIds(Lists.newArrayList(SCHEMA_1))
                .build(), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, false);
    }

    @Test
    public void shouldBePublicWhenAllGraphsDefaultedPrivateAndGraphIsSetPublic() throws Exception {
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(getAddGraphOp(true), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, true);
    }


    @Test
    public void shouldNotBePublicWhenAllGraphsDefaultedPrivateAndGraphIsSetPrivate() throws Exception {
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(getAddGraphOp(false), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, false);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPrivateAndGraphIsSetPublic() throws Exception {
        fedProps.setFalseGraphsCanHavePublicAccess();
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(getAddGraphOp(true), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, false);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPrivateAndGraphIsSetPrivate() throws Exception {
        fedProps.setFalseGraphsCanHavePublicAccess();
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(getAddGraphOp(false), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, false);
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPublicAndGraphIsSetPrivate() throws Exception {
        fedProps.setTrueGraphsCanHavePublicAccess();
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(getAddGraphOp(false), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, false);
    }

    @Test
    public void shouldBePublicWhenAllGraphsSetPublicAndGraphIsSetPublic() throws Exception {
        fedProps.setTrueGraphsCanHavePublicAccess();
        store.initialise(TEST_FED_STORE_ID, null, fedProps);
        store.execute(getAddGraphOp(true), TEST_USER_CONTEXT);
        getAllGraphsIdsHasNext(store, BLANK_USER_CONTEXT, true);
    }


    private AddGraph getAddGraphOp(final boolean isPublic) {
        return new AddGraph.Builder()
                .isPublic(isPublic)
                .graphId(GRAPH_1)
                .parentPropertiesId(PROP_1)
                .parentSchemaIds(Lists.newArrayList(SCHEMA_1))
                .build();
    }

    private static void getAllGraphsIdsHasNext(FederatedStore store, Context blankUserContext, final boolean expected) throws uk.gov.gchq.gaffer.operation.OperationException {
        Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankUserContext);
        //final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankUserContext);
        assertEquals(expected, execute.iterator().hasNext());
    }
}
