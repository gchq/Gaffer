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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;

public class FederatedAccessPublicAccessTest {


    @Test
    public void shouldHavePublicAccess() throws Exception {
        final FederatedAccess access = new FederatedAccess.Builder()
                .makePublic()
                .build();

        assertTrue(access.hasReadAccess(blankUser()));
    }

    @Test
    public void shouldHavePrivateAccess() throws Exception {

        final FederatedAccess access = new FederatedAccess.Builder()
                .makePrivate()
                .build();

        assertFalse(access.hasReadAccess(blankUser()));
    }

}
