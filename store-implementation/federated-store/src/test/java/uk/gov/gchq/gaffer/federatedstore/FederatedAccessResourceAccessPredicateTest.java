/*
 * Copyright 2020 Crown Copyright
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
import uk.gov.gchq.gaffer.access.predicate.CustomAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphReadAccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphWriteAccessPredicate;
import uk.gov.gchq.gaffer.user.User;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.ALL_USERS;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedAccessResourceAccessPredicateTest {

    private static final AccessPredicate READ_ACCESS_PREDICATE = new CustomAccessPredicate("CreatingUserId", singletonMap("ReadKey", "ReadValue"), asList("CustomReadAuth1", "CustomReadAuth2"));
    private static final AccessPredicate WRITE_ACCESS_PREDICATE = new CustomAccessPredicate("CreatingUserId", singletonMap("WriteKey", "WriteValue"), asList("CustomWriteAuth1", "CustomWriteAuth2"));
    private final User testUser = testUser();

    @Test
    public void shouldConfigureDefaultFederatedGraphAccessPredicatesWhenNoAccessPredicateConfigurationSupplied() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(testUser.getUserId())
                .graphAuths(ALL_USERS)
                .build();

        final AccessPredicate expectedNonPublicReadAccessPredicate = new FederatedGraphReadAccessPredicate(testUser.getUserId(), asList(ALL_USERS), false);
        final AccessPredicate expectedWriteAccessPredicate = new FederatedGraphWriteAccessPredicate(testUser.getUserId());

        assertEquals(expectedNonPublicReadAccessPredicate, access.getReadAccessPredicate());
        assertEquals(expectedWriteAccessPredicate, access.getWriteAccessPredicate());

        final FederatedAccess publicAccess = new FederatedAccess.Builder()
                .addingUserId(testUser.getUserId())
                .graphAuths(ALL_USERS)
                .makePublic()
                .build();

        final AccessPredicate expectedPublicReadAccessPredicate = new FederatedGraphReadAccessPredicate(testUser.getUserId(), asList(ALL_USERS), true);

        assertEquals(expectedPublicReadAccessPredicate, publicAccess.getReadAccessPredicate());
        assertEquals(expectedWriteAccessPredicate, publicAccess.getWriteAccessPredicate());
    }

    @Test
    public void shouldNotAllowReadAccessWhenNoAccessPredicateConfigured() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(testUser.getUserId())
                .graphAuths(ALL_USERS)
                .readAccessPredicate(new NoAccessPredicate())
                .build();

        assertFalse(access.hasReadAccess(testUser));
        assertTrue(access.hasWriteAccess(testUser));
    }

    @Test
    public void shouldNotAllowWriteAccessWhenNoAccessPredicateConfigured() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(testUser.getUserId())
                .graphAuths(ALL_USERS)
                .writeAccessPredicate(new NoAccessPredicate())
                .build();

        assertTrue(access.hasReadAccess(testUser));
        assertFalse(access.hasWriteAccess(testUser));
    }

    @Test
    public void shouldBeFederatedStoreGraphResourceType() {
        assertEquals(ResourceType.FederatedStoreGraph, new FederatedAccess.Builder().build().getResourceType());
    }

    @Test
    public void shouldReturnDeduplicatedSortedListOfAuths() {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths("a", "z", "b", "c")
                .writeAccessPredicate(new CustomAccessPredicate("CreatingUserId", emptyMap(), asList("z", "x", "a", "b")))
                .build();
        assertEquals(asList("a", "b", "c", "x", "z"), access.getAuths());
    }
}
