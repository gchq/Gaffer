/*
 * Copyright 2020-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphReadAccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphWriteAccessPredicate;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.CACHE_SERVICE_CLASS_DEFAULT;
import static uk.gov.gchq.gaffer.user.StoreUser.ALL_USERS;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedAccessResourceAccessPredicateTest {

    @Test
    public void shouldConfigureDefaultFederatedGraphAccessPredicatesWhenNoAccessPredicateConfigurationSupplied() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .owningUserId(TEST_USER_ID)
                .graphAuths(ALL_USERS)
                .build();

        final AccessPredicate expectedNonPublicReadAccessPredicate = new FederatedGraphReadAccessPredicate(TEST_USER_ID, asList(ALL_USERS), false);
        final AccessPredicate expectedWriteAccessPredicate = new FederatedGraphWriteAccessPredicate(TEST_USER_ID);

        assertEquals(expectedNonPublicReadAccessPredicate, access.getOrDefaultReadAccessPredicate());
        assertEquals(expectedWriteAccessPredicate, access.getOrDefaultWriteAccessPredicate());

        final FederatedAccess publicAccess = new FederatedAccess.Builder()
                .owningUserId(TEST_USER_ID)
                .graphAuths(ALL_USERS)
                .makePublic()
                .build();

        final AccessPredicate expectedPublicReadAccessPredicate = new FederatedGraphReadAccessPredicate(TEST_USER_ID, asList(ALL_USERS), true);

        assertEquals(expectedPublicReadAccessPredicate, publicAccess.getOrDefaultReadAccessPredicate());
        assertEquals(expectedWriteAccessPredicate, publicAccess.getOrDefaultWriteAccessPredicate());
    }

    @Test
    public void shouldNotAllowReadAccessWhenNoAccessPredicateConfigured() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .owningUserId(TEST_USER_ID)
                .readAccessPredicate(new NoAccessPredicate())
                .build();

        assertFalse(access.hasReadAccess(testUser()));
        assertTrue(access.hasWriteAccess(testUser()));
    }

    @Test
    public void shouldNotAllowWriteAccessWhenNoAccessPredicateConfigured() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .owningUserId(TEST_USER_ID)
                .graphAuths(ALL_USERS)
                .writeAccessPredicate(new NoAccessPredicate())
                .build();

        assertTrue(access.hasReadAccess(testUser()));
        assertFalse(access.hasWriteAccess(testUser()));
    }

    @Test
    public void shouldBeFederatedStoreGraphResourceType() {
        assertEquals(ResourceType.FederatedStoreGraph, new FederatedAccess.Builder().build().getResourceType());
    }

    @Test
    public void shouldBeSerialisableUsingCacheWhenUsingCustomPredicate() throws Exception {
        // Given
        FederatedAccess original = new FederatedAccess.Builder()
                .owningUserId(TEST_USER_ID)
                .graphAuths(ALL_USERS)
                .writeAccessPredicate(new AccessPredicate(new CustomUserPredicate()))
                .build();

        CacheServiceLoader.initialise(CACHE_SERVICE_CLASS_DEFAULT);
        final FederatedStoreCache testCache = new FederatedStoreCache("shouldBeSerialisableWithCacheWhenUsingCustomPredicate");
        final String testKey = "testKey";

        // When
        testCache.addGraphToCache(new GraphSerialisable(new GraphConfig(testKey), new Schema(), new StoreProperties()), original, false);
        FederatedAccess cached = testCache.getAccessFromCache(testKey);

        // Then
        assertEquals(original, cached);
    }

    private static byte[] serialise(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    private static Object deserialise(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
}
