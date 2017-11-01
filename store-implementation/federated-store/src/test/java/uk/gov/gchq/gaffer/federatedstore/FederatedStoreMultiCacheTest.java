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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FederatedStoreMultiCacheTest {

    public static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    public static final String MAP_ID_1 = "mockMapGraphId1";
    public static final String PATH_MAP_STORE_PROPERTIES = "properties/singleUseMockMapStore.properties";
    public static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    public static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    public static User authUser = FederatedStoreUser.authUser();
    public static User testUser = FederatedStoreUser.testUser();
    public FederatedStore store;
    public FederatedStoreProperties federatedStoreProperties;
    public Collection<String> originalStoreIds;
    public FederatedStore store2;
    public User blankUser;

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
                .graphId(MAP_ID_1)
                .graphAuths(FederatedStoreUser.AUTH_1)
                .isPublic(false)
                .storeProperties(MapStoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES))
                .schema(Schema.fromJson(StreamUtil.openStream(Schema.class, PATH_BASIC_ENTITY_SCHEMA_JSON)))
                .build(), new Context.Builder()
                .user(testUser)
                .build());


        store2 = new FederatedStore();
        store2.initialise(FEDERATED_STORE_ID + 1, null, federatedStoreProperties);
        blankUser = FederatedStoreUser.blankUser();
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
                .graphId(MAP_ID_1 + 1)
                .isPublic(true)
                .storeProperties(MapStoreProperties.loadStoreProperties(PATH_MAP_STORE_PROPERTIES))
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
