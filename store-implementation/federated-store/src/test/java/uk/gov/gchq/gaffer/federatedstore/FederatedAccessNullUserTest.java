/*
 * Copyright 2017-2018 Crown Copyright
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

import java.util.Collection;

import static org.junit.Assert.assertFalse;

public class FederatedAccessNullUserTest {

    @Test
    public void shouldNeverValidateNullUserI() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .build();

        assertFalse(access.isValidToExecute(null));
    }

    @Test
    public void shouldNeverValidateNullUserII() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths((String[]) null)
                .build();

        assertFalse(access.isValidToExecute(null));
    }

    @Test
    public void shouldNeverValidateNullUserIII() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths((Collection<String>) null)
                .build();

        assertFalse(access.isValidToExecute(null));
    }

    @Test
    public void shouldNeverValidateNullUserIV() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .addingUserId(null)
                .build();
        assertFalse(access.isValidToExecute(null));
    }

    @Test
    public void shouldNeverValidateNullUserV() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .graphAuths((String[]) null)
                .addingUserId(null)
                .build();
        assertFalse(access.isValidToExecute(null));
    }

}
