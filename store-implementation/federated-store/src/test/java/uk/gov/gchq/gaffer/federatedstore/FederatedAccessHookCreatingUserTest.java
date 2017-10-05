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

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.gaffer.user.User.Builder;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertTrue;

/**
 * The user that created the graph should still have visibility/access of the graph,
 * they created. However other mechanisms will stop them from
 * performing operations that they do not have, else where in code.
 */
public class FederatedAccessHookCreatingUserTest {

    public static final String A = "A";
    public static final String B = "B";
    public static final String USER = "user";

    @Test
    public void shouldValidateWithWrongAuth() throws Exception {

        final User user = new Builder()
                .opAuth(A)
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(B)
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithNoAuth() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(B)
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithNullHookAuthCollection() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths((Collection) null)
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithNullHookAuthStringArray() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths((String[]) null)
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthCollection() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(new HashSet<>())
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthStringArray() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(new String[0])
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthCollectionII() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(Sets.newHashSet(""))
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

    @Test
    public void shouldValidateWithEmptyHookAuthStringArrayII() throws Exception {

        final User user = new Builder()
                .userId(USER)
                .build();

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths("")
                .build();

        hook.setAddingUserId(USER);

        assertTrue(hook.isValidToExecute(new Context(user)));
    }

}
