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

import static org.junit.Assert.assertFalse;

public class FederatedAccessHookNullEmptyTest {

    @Test
    public void shouldInValidateWithExplicitlyNullCollectionAuth() throws Exception {

        final Context context = new Context(new Builder()
                .build());

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths((Collection) null)
                .build();

        assertFalse(hook.isValidToExecute(context));
    }

    @Test
    public void shouldInValidateWithExplicitlyNullStringsAuth() throws Exception {

        final Context context = new Context(new Builder()
                .build());

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths((String[]) null)
                .build();

        assertFalse(hook.isValidToExecute(context));
    }

    @Test
    public void shouldInValidateWithEmptyStringAuth() throws Exception {

        final Context context = new Context(new Builder()
                .build());

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths("")
                .build();

        assertFalse(hook.isValidToExecute(context));
    }

    @Test
    public void shouldInValidateWithEmptyStringsAuth() throws Exception {

        final Context context = new Context(new Builder()
                .build());

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(new String[0])
                .build();

        assertFalse(hook.isValidToExecute(context));
    }

    @Test
    public void shouldInValidateWithEmptyCollectionAuth() throws Exception {

        final Context context = new Context(new Builder()
                .build());

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .graphAuths(Sets.newHashSet())
                .build();

        assertFalse(hook.isValidToExecute(context));
    }


  @Test
    public void shouldInValidateWithUnSetAuth() throws Exception {

      final Context context = new Context(new Builder()
              .build());

        final FederatedAccessHook hook = new FederatedAccessHook.Builder()
                .build();

        assertFalse(hook.isValidToExecute(context));
    }

}
