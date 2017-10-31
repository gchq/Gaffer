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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedGetTraitsHandlerTest {

    public static final String MAP_STORE = "mapStore";
    public static final String FED_STORE_ID = "fedStoreId";
    private FederatedStore federatedStore;

    @Before
    public void setUp() throws Exception {
        federatedStore = new FederatedStore();
    }

    @Test
    public void shouldHaveNoTraitsForEmptyStore() throws Exception {
        federatedStore.initialise(FED_STORE_ID, null, new FederatedStoreProperties());
        Iterable<? extends StoreTrait> execute = federatedStore.execute(new GetTraits.Builder()
                .build(), new Context(testUser()));

        assertNotNull(execute);
        assertFalse(execute.iterator().hasNext());
    }

    @Test
    public void shouldHaveAllTraitsForSupported() throws Exception {
        federatedStore.initialise(FED_STORE_ID, null, new FederatedStoreProperties());
        Iterable<? extends StoreTrait> execute = federatedStore.execute(new GetTraits.Builder()
                .currentlyAvailableTraits(false)
                .build(), new Context(testUser()));

        assertNotNull(execute);
        Iterator<? extends StoreTrait> iterator = execute.iterator();
        assertTrue(iterator.hasNext());

        Set<StoreTrait> allTraits = StoreTrait.ALL_TRAITS;
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(allTraits.contains(iterator.next()));
            count++;
        }
        assertEquals(allTraits.size(), count);
    }

    @Test
    public void shouldHaveTraitsWhenContainsMapStore() throws Exception {
        federatedStore.initialise(FED_STORE_ID, null, new FederatedStoreProperties());
        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(MAP_STORE)
                .storeProperties(new MapStoreProperties())
                .schema(new Schema())
                .build(), new Context(testUser()));

        Iterable<? extends StoreTrait> execute = federatedStore.execute(new GetTraits.Builder()
                .build(), new Context(testUser()));

        assertNotNull(execute);
        Iterator<? extends StoreTrait> iterator = execute.iterator();
        assertTrue(iterator.hasNext());
        Set<StoreTrait> allTraits = MapStore.TRAITS;
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(allTraits.contains(iterator.next()));
            count++;
        }
        assertEquals(allTraits.size(), count);
    }

    @Test
    public void shouldHaveTraitsWhenContainsMapStoreWithOptions() throws Exception {
        federatedStore.initialise(FED_STORE_ID, null, new FederatedStoreProperties());
        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(MAP_STORE)
                .storeProperties(new MapStoreProperties())
                .schema(new Schema())
                .build(), new Context(testUser()));

        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId("accStore")
                .storeProperties(StoreProperties.loadStoreProperties("/properties/singleUseMockAccStore.properties"))
                .schema(new Schema())
                .build(), new Context(testUser()));

        Iterable<? extends StoreTrait> execute = federatedStore.execute(new GetTraits.Builder()
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, MAP_STORE)
                .build(), new Context(testUser()));

        assertNotNull(execute);
        Iterator<? extends StoreTrait> iterator = execute.iterator();
        assertTrue(iterator.hasNext());
        Set<StoreTrait> allTraits = MapStore.TRAITS;
        int count = 0;
        while (iterator.hasNext()) {
            assertTrue(allTraits.contains(iterator.next()));
            count++;
        }
        assertEquals(allTraits.size(), count);
    }

}