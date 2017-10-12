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

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedStoreAuthTest {

    private User testUser;
    private User authUser;

    @Before
    public void setUp() throws Exception {
        testUser = testUser();
        authUser = authUser();
    }

    @Test
    public void shouldAddGraphWithAuth() throws Exception {

        FederatedStore store = new FederatedStore();

        Schema expectedSchema = new Schema.Builder().build();
        String expectedGraphId = "testGraphID";

        FederatedStoreProperties storeProperties = new FederatedStoreProperties();
        storeProperties.setFalseSkipFailedExecution();

        assertEquals(0, store.getGraphs(testUser, null).size());

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(expectedGraphId)
                        .schema(expectedSchema)
                        .storeProperties(storeProperties)
                        .graphAuths("auth1")
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(authUser, null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(expectedGraphId, next.getGraphId());
        assertEquals(expectedSchema, next.getSchema());

         graphs = store.getGraphs(blankUser(), null);

        assertNotNull(graphs);
        assertTrue(graphs.isEmpty());
    }


}