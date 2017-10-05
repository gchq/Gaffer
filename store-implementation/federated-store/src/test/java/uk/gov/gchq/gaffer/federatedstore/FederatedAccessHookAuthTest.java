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

import org.junit.Test;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.gaffer.user.User.Builder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FederatedAccessHookAuthTest {

    public static final String A = "A";
    public static final String B = "B";
    public static final String AA = "AA";
    public static final String USER = "user";

    @Test
    public void shouldValidateUserWithMatchingAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A)
                .build();

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateUserWithSubsetAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A, B)
                .build();

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateUserWithSurplusMatchingAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .opAuth(B)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A)
                .build();

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldInValidateUserWithNoAuth() throws Exception {

        final User user = new Builder()
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A)
                .build();

        assertFalse(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldInValidateUserWithMismatchedAuth() throws Exception {

        final User user = new Builder()
                .opAuth(B)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A)
                .build();

        assertFalse(hook.isValidToExecute(new Context(user)));
    }

}
