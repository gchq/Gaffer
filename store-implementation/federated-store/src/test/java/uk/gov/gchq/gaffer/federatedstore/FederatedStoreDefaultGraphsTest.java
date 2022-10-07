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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadFederatedStoreFrom;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getCleanStrings;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreDefaultGraphsTest {

    @Disabled
    @Test
    public void testDisableByDefault() {
        fail("Not yet implemented");
    }

    @Disabled
    @Test
    public void testDisableByDefaultAdmin() {
        fail("Not yet implemented");
    }

    @Disabled
    @Test
    public void testDisableByDefaultButIsDefaultListOfGraphs() {
        fail("Not yet implemented");
    }

    @Test
    public void shouldGetDefaultedGraphIdFromJsonConfig() throws Exception {
        //Given
        FederatedStore federatedStore = loadFederatedStoreFrom("DefaultedGraphIds.json");
        assertThat(federatedStore)
                .isNotNull()
                .returns(Lists.newArrayList("defaultJsonGraphId"), from(FederatedStore::getAdminConfiguredDefaultGraphIds));

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
                .returns(Lists.newArrayList("defaultJsonGraphId"), from(FederatedStore::getAdminConfiguredDefaultGraphIds));

        //when
        federatedStore.setAdminConfiguredDefaultGraphIdsCSV("other");

        //then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo()));
        assertThat(exception).message().contains("The following graphIds are not visible or do not exist: [defaultJsonGraphId]");
    }
}
