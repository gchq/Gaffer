/*
 * Copyright 2021 Crown Copyright
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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreDefaultGraphsTest {

    @Test
    public void shouldGetDefaultedGraphIdFromJsonConfig() throws Exception {
        //Given
        FederatedStore federatedStore = JSONSerialiser.deserialise(IOUtils.toByteArray(StreamUtil.openStream(this.getClass(), "DefaultedGraphIds.json")), FederatedStore.class);
        assertNotNull(federatedStore);
        assertEquals("defaultJsonGraphId", federatedStore.getAdminConfiguredDefaultGraphIdsCSV());

        try {
            //when
            federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo());
        } catch (final Exception e) {
            //then
            try {
                assertTrue(e.getMessage().contains("defaultJsonGraphId"));
                assertEquals("The following graphIds are not visible or do not exist: [defaultJsonGraphId]", e.getMessage());
            } catch (final Exception e2) {
                throw e;
            }
        }

    }

    @Test
    public void shouldNotChangeExistingDefaultedGraphId() throws Exception {
        //Given
        FederatedStore federatedStore = JSONSerialiser.deserialise(IOUtils.toByteArray(StreamUtil.openStream(this.getClass(), "DefaultedGraphIds.json")), FederatedStore.class);
        assertNotNull(federatedStore);
        assertEquals("defaultJsonGraphId", federatedStore.getAdminConfiguredDefaultGraphIdsCSV());

        //when
        federatedStore.setAdminConfiguredDefaultGraphIdsCSV("other");

        //then
        try {
            federatedStore.getGraphs(testUser(), null, new GetAllGraphInfo());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("defaultJsonGraphId"));
            assertEquals("The following graphIds are not visible or do not exist: [defaultJsonGraphId]", e.getMessage());
        }
    }
}
