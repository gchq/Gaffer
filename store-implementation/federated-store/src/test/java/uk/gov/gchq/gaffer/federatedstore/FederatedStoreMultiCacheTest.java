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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloTestClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreMultiCacheTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedStoreMultiCacheTest.class);
    public static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    public static final String ACC_ID_1 = "miniAccGraphId1";
    public static final String PATH_ACC_STORE_PROPERTIES = "properties/singleUseMiniAccStore.properties";
    public static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    public static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    public static User authUser = authUser();
    public static User testUser = testUser();
    public FederatedStore store;
    public FederatedStoreProperties federatedStoreProperties;
    public Collection<String> originalStoreIds;
    public FederatedStore store2;
    public User blankUser;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, PATH_ACC_STORE_PROPERTIES));
    private static AccumuloTestClusterManager accumuloTestClusterManager;

    @ClassRule
    public static TemporaryFolder storeBaseFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setUpStore() {
        File storeFolder = null;
        try {
            storeFolder = storeBaseFolder.newFolder();
        } catch (IOException e) {
            LOGGER.error("Failed to create sub folder in : " + storeBaseFolder.getRoot().getAbsolutePath() + ": " + e.getMessage());
        }
        accumuloTestClusterManager = new AccumuloTestClusterManager(PROPERTIES, storeFolder.getAbsolutePath());
    }

    @AfterClass
    public static void tearDownStore() {
        accumuloTestClusterManager.close();
    }

    @Before
    public void setUp() throws Exception {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        federatedStoreProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));
        store = new FederatedStore();
        store.initialise(FEDERATED_STORE_ID, null, federatedStoreProperties);
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_1)
                .graphAuths(AUTH_1)
                .isPublic(false)
                .storeProperties(PROPERTIES)
                .schema(Schema.fromJson(StreamUtil.openStream(Schema.class, PATH_BASIC_ENTITY_SCHEMA_JSON)))
                .build(), new Context.Builder()
                .user(testUser)
                .build());


        store2 = new FederatedStore();
        store2.initialise(FEDERATED_STORE_ID + 1, null, federatedStoreProperties);
        blankUser = blankUser();
    }

    @After
    public void after() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldInitialiseByCacheToContainSameGraphsForAddingUser() throws Exception {
        originalStoreIds = store.getAllGraphIds(testUser);
        final int firstStoreSize = originalStoreIds.size();
        assertEquals("adding user should have visibility of first store graphs", 1, firstStoreSize);
        Collection<String> storeGetIds2 = store2.getAllGraphIds(testUser);
        assertEquals("adding user should have same visibility of second store graphs", firstStoreSize, storeGetIds2.size());
        assertTrue(originalStoreIds.containsAll(storeGetIds2));
    }

    @Test
    public void shouldInitialiseByCacheToContainSameGraphsForAuthUser() throws Exception {
        originalStoreIds = store.getAllGraphIds(authUser);
        final int firstStoreSize = originalStoreIds.size();

        assertEquals("auth user should have visibility of first store graphs", 1, firstStoreSize);
        Collection<String> storeGetIds2 = store2.getAllGraphIds(authUser);
        assertEquals("auth user should have same visibility of second store graphs", firstStoreSize, storeGetIds2.size());
        assertTrue(originalStoreIds.containsAll(storeGetIds2));
    }

    @Test
    public void shouldInitialiseByCacheToContainSameGraphsForBlankUser() throws Exception {
        originalStoreIds = store.getAllGraphIds(blankUser);
        final int firstStoreSize = originalStoreIds.size();

        assertEquals("There should be 1 graphs", 1, store.getAllGraphIds(testUser).size());

        assertEquals("blank user should not have visibility of first store graphs", 0, firstStoreSize);
        Collection<String> storeGetIds2 = store2.getAllGraphIds(blankUser);
        assertEquals("blank user should have same visibility of second store graphs", firstStoreSize, storeGetIds2.size());
        assertEquals("blank user should have same visibility of second store graphs", firstStoreSize, storeGetIds2.size());
        assertTrue(originalStoreIds.containsAll(storeGetIds2));
    }


    @Test
    public void shouldInitialiseByCacheToContainSamePublicGraphsForBlankUser() throws Exception {
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_1 + 1)
                .isPublic(true)
                .storeProperties(PROPERTIES)
                .schema(Schema.fromJson(StreamUtil.openStream(Schema.class, PATH_BASIC_ENTITY_SCHEMA_JSON)))
                .build(), new Context.Builder()
                .user(testUser)
                .build());

        store2 = new FederatedStore();
        store2.initialise(FEDERATED_STORE_ID + 1, null, federatedStoreProperties);

        assertEquals("There should be 2 graphs", 2, store.getAllGraphIds(testUser).size());

        originalStoreIds = store.getAllGraphIds(blankUser);
        final int firstStoreSize = originalStoreIds.size();

        assertEquals("blank user should have visibility of public graph", 1, firstStoreSize);
        Collection<String> storeGetIds2 = store2.getAllGraphIds(blankUser);
        assertEquals("blank user should have same visibility of second store graphs", firstStoreSize, storeGetIds2.size());
        assertTrue(originalStoreIds.containsAll(storeGetIds2));
    }
}
