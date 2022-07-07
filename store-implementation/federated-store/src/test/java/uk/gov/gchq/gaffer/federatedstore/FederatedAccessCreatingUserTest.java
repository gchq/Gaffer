/*
 * Copyright 2017-2022 Crown Copyright
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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

/**
 * The user that created the graph should still have visibility/access of the
 * graph,
 * they created. However other mechanisms will stop them from
 * performing operations that they do not have, else where in code.
 */
public class FederatedAccessCreatingUserTest {


    @Test
    public void shouldValidateWithWrongAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths(AUTH_1)
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithNoAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths(AUTH_1)
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithNullHookAuthCollection() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths((Collection) null)
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithNullHookAuthStringArray() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths((String[]) null)
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthCollection() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths(new HashSet<>())
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthStringArray() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths(new String[0])
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthCollectionII() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths(Sets.newHashSet(""))
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthStringArrayII() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(TEST_USER_ID)
                .graphAuths("")
                .build();

        assertTrue(access.hasReadAccess(testUser()));
    }

}
