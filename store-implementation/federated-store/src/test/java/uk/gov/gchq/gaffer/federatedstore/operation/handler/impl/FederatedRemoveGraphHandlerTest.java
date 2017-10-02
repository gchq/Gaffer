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

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedRemoveGraphHandlerTest {

    private User testUser;

    @Before
    public void setUp() throws Exception {
        testUser = testUser();
    }

    @Test
    public void shouldRemoveGraph() throws Exception {
        String graphId = "testGraphId";
        AccumuloProperties storeProperties = new AccumuloProperties();
        storeProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        FederatedStore store = new FederatedStore();
        store.addGraphs(testUser.getOpAuths(), null, new Graph.Builder()
                .config(new GraphConfig(graphId))
                .addSchema(new Schema.Builder().build())
                .storeProperties(storeProperties)
                .build());

        assertEquals(1, store.getGraphs(testUser, null).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .setGraphId(graphId)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

        assertEquals(0, graphs.size());

    }
}