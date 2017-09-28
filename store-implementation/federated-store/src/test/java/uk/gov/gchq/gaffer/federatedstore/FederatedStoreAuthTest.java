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

import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetAllElementsHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FederatedStoreAuthTest {

    @Test
    public void shouldAddGraphWithHook() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String federatedGraphId = "federatedStoreGraphId";
        String expectedGraphId = "testGraphID";

        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set("gaffer.store.class", "uk.gov.gchq.gaffer.federatedstore.FederatedStore");
        storeProperties.set(CacheProperties.CACHE_SERVICE_CLASS, "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService");

        assertEquals(0, store.getGraphs(null).size());

        store.initialise(federatedGraphId, null, storeProperties);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .graphAuths("auth1")
                        .build(),
                new Context(new User("TestUser")),
                store);

        Collection<Graph> graphs = store.getGraphs(null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

        final FederatedGetAllElementsHandler federatedGetAllElementsHandler = new FederatedGetAllElementsHandler();

        try {
            federatedGetAllElementsHandler.doOperation(
                    new GetAllElements(),
                    new Context(new User("TestUser")),
                    store);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains(String.format(FederatedAccessHook.USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S, "")));
        }

    }


}