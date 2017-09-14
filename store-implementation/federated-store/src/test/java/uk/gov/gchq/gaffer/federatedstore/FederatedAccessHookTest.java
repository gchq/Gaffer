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

import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.gaffer.user.User.Builder;

public class FederatedAccessHookTest {

    public static final String A = "A";
    public static final String B = "B";
    public static final String AA = "AA";
    public static final String USER = "user";

    //At least 1 ops.

    @Test
    public void shouldValidateMatchingAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A)
                .build();

        Assert.assertTrue(hook.isValidToExecute(user));
    }

    @Test
    public void shouldInValidateNoAuthNoUser() throws Exception {

        final User user = new Builder()
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .build();

        Assert.assertFalse(hook.isValidToExecute(user));
    }

    @Test
    public void shouldInValidateMismatchingAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(B)
                .build();

        Assert.assertFalse(hook.isValidToExecute(user));
    }

    @Test
    public void shouldInValidateMissingAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .build();

        Assert.assertFalse(hook.isValidToExecute(user));
    }

    @Test
    public void shouldValidatePartialMatchingAuth() throws Exception {

        final User user = new Builder()
                .opAuths(A, AA)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A)
                .build();

        Assert.assertTrue(hook.isValidToExecute(user));
    }

    @Test
    public void shouldValidateMatchingAuthWithSurplus() throws Exception {

        final User user = new Builder()
                .opAuths(A)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(A, AA)
                .build();

        Assert.assertTrue(hook.isValidToExecute(user));
    }

    /**
     * The user that created the graph should still have visibility/access of the graph,
     * they created, however other mechanisms will stop them from
     * performing operations that they do not have.
     *
     * @throws Exception
     */
    @Test
    public void shouldValidateCreatingUserRegardlessOfAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(B)
                .build();

        hook.setAddingUserId(USER);

        Assert.assertTrue(hook.isValidToExecute(user));
    }

    @Test
    public void shouldValidateWithExplicitlySetEmptyPublicAuth() throws Exception {

        final User user = new Builder()
                .build();

        String s = null;

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(s)
                .graphAuths("")
                .build();

        Assert.assertTrue(hook.isValidToExecute(user));
    }


}