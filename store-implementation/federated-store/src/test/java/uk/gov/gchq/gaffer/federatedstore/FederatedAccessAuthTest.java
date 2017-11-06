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

import com.beust.jcommander.internal.Lists;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.ALL_USERS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedAccessAuthTest {

    public static final String AuthX = "X";

    User testUser;

    @Before
    public void setUp() throws Exception {
        testUser = testUser();
    }

    @Test
    public void shouldValidateUserWithMatchingAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS)
                .build();

        assertTrue(access.isValidToExecute(testUser));
    }

    @Test
    public void shouldValidateUserWithSubsetAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS, AuthX)
                .build();

        assertTrue(access.isValidToExecute(testUser));
    }

    @Test
    public void shouldValidateUserWithSurplusMatchingAuth() throws Exception {
        final User user = authUser();

        assertTrue(user.getOpAuths().contains(AUTH_1));

        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS)
                .build();

        assertTrue(access.isValidToExecute(user));
    }

    @Test
    public void shouldInValidateUserWithNoAuth() throws Exception {

        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths(ALL_USERS)
                .build();

        assertFalse(access.isValidToExecute(blankUser()));
    }

    @Test
    public void shouldInValidateUserWithMismatchedAuth() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths("X")
                .build();

        assertFalse(access.isValidToExecute(testUser));
    }

    @Test
    public void shouldValidateWithListOfAuths() throws Exception {

        final FederatedAccess access = new FederatedAccess.Builder()
                .addGraphAuths(Lists.newArrayList(AUTH_1))
                .addGraphAuths(Lists.newArrayList("X"))
                .build();

        assertTrue(access.isValidToExecute(authUser()));

    }
}
