/*
 * Copyright 2021-2022 Crown Copyright
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadFederatedStoreFrom;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreDefaultGraphsTest {

    @Disabled //TODO FS remove ignore
    @Test
    public void testDisableByDefault() {
        fail();
    }

    @Disabled //TODO FS remove ignore
    @Test
    public void testDisableByDefaultAdmin() {
        fail();
    }

    @Disabled //TODO FS remove ignore
    @Test
    public void testDisableByDefaultButIsDefaultListOfGraphs() {
        fail();
    }

    @Test
    public void shouldGetDefaultedGraphIdFromJsonConfig() throws Exception {
        //Given
        FederatedStore federatedStore = loadFederatedStoreFrom("DefaultedGraphIds.json");
        assertThat(federatedStore)
                .isNotNull()
                .returns("defaultJsonGraphId", from(FederatedStore::getAdminConfiguredDefaultGraphIdsCSV));

        //when
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo()));
        //then
        assertThat(exception).message().contains("The following graphIds are not visible or do not exist: [defaultJsonGraphId]");
    }

    @Test
    public void shouldNotChangeExistingDefaultedGraphId() throws Exception {
        //Given
        FederatedStore federatedStore = loadFederatedStoreFrom("DefaultedGraphIds.json");
        assertThat(federatedStore)
                .isNotNull()
                .returns("defaultJsonGraphId", from(FederatedStore::getAdminConfiguredDefaultGraphIdsCSV));

        //when
        federatedStore.setAdminConfiguredDefaultGraphIdsCSV("other");

        //then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo()));
        assertThat(exception).message().contains("The following graphIds are not visible or do not exist: [defaultJsonGraphId]");
    }
}
