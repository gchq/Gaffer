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

package uk.gov.gchq.gaffer.federated.simple.access;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;

class GraphAccessTest {

    @Test
    void shouldPreventNonAdminReadIfGraphIsPrivate() {
        // Given
        // Private graph with a non admin user
        GraphAccess access = new GraphAccess.Builder()
            .isPublic(false)
            .readAccessPredicate(new NoAccessPredicate()).build();
        User testUser = new User("test");

        // Then
        assertThat(access.hasReadAccess(testUser)).isFalse();
        assertThat(access.hasReadAccess(testUser, "adminAuth")).isFalse();
    }

    @Test
    void shouldAllowAdminUserToReadPrivateGraphs() {
        // Given
        String adminAuth = "admin";
        // Private graph with admin user
        GraphAccess access = new GraphAccess.Builder()
            .isPublic(false)
            .readAccessPredicate(new AccessPredicate(new DefaultUserPredicate(User.UNKNOWN_USER_ID, null)))
            .build();
        User adminUser = new User.Builder()
            .opAuth(adminAuth)
            .userId("adminUser").build();

        // Then
        // Pass in admin auth to check
        assertThat(access.hasReadAccess(adminUser, adminAuth)).isTrue();
    }

    @Test
    void shouldAllowSystemUserToReadPrivateGraphs() {
        // Given
        // Private graph with no read access
        GraphAccess access = new GraphAccess.Builder()
            .isPublic(false)
            .readAccessPredicate(new NoAccessPredicate()).build();

        // System user for federated stores
        User systemUser = new User(FederatedStore.FEDERATED_STORE_SYSTEM_USER);

        // Then
        assertThat(access.hasReadAccess(systemUser)).isTrue();
    }

    @Test
    void shouldAllowAnyUserToReadPublicGraphs() {
        // Given
        // Public graph with restricted read access predicate
        GraphAccess access = new GraphAccess.Builder()
            .isPublic(true)
            .readAccessPredicate(new NoAccessPredicate()).build();

        User unknownUser = new User(User.UNKNOWN_USER_ID);

        // Then
        assertThat(access.hasReadAccess(unknownUser)).isTrue();
    }

    @Test
    void shouldPreventWritingToGraphViaPredicate() {
        // Given
        // Graph that does not allow writing
        GraphAccess access = new GraphAccess.Builder()
            .writeAccessPredicate(new NoAccessPredicate()).build();

        User testUser = new User("test");

        // When
        assertThat(access.hasWriteAccess(testUser)).isFalse();
    }


    @Test
    void shouldAllowSystemUserToWriteToGraphs() {
        // Given
        // Graph that does not allow writing
        GraphAccess access = new GraphAccess.Builder()
                .writeAccessPredicate(new NoAccessPredicate()).build();

        // System user for federated stores
        User systemUser = new User(FederatedStore.FEDERATED_STORE_SYSTEM_USER);

        // Then
        assertThat(access.hasWriteAccess(systemUser)).isTrue();
    }

}
