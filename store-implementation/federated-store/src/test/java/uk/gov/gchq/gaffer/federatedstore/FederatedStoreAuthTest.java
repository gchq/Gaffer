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

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedStoreAuthTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";

    private final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
    private User testUser;
    private User authUser;
    private FederatedStore federatedStore;
    private FederatedStoreProperties federatedStoreProperties;
    private MapStoreProperties graphStoreProperties;
    private Schema schema;

    @Before
    public void setUp() throws Exception {
        testUser = testUser();
        authUser = authUser();

        CacheServiceLoader.shutdown();
        federatedStore = new FederatedStore();

        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        graphStoreProperties = new MapStoreProperties();

        schema = new Schema.Builder().build();
    }

    @Test
    public void shouldAddGraphWithAuth() throws Exception {
        federatedStore.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(schema)
                        .storeProperties(graphStoreProperties)
                        .graphAuths("auth1")
                        .build(),
                new Context(testUser),
                federatedStore);

        Collection<Graph> graphs = federatedStore.getGraphs(authUser, null);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(schema, next.getSchema());

        graphs = federatedStore.getGraphs(blankUser(), null);

        assertNotNull(graphs);
        assertTrue(graphs.isEmpty());
    }

    @Test
    public void shouldNotShowHiddenGraphsInError() throws Exception {
        federatedStore.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        final String unusualType = "unusualType";
        final String groupEnt = "ent";
        final String groupEdge = "edg";
        schema = new Schema.Builder()
                .type(unusualType, String.class)
                .entity(groupEnt, new SchemaEntityDefinition.Builder()
                        .vertex(unusualType)
                        .build())
                .edge(groupEdge, new SchemaEdgeDefinition.Builder()
                        .source(unusualType)
                        .destination(unusualType)
                        .build())
                .build();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(schema)
                        .storeProperties(graphStoreProperties)
                        .graphAuths("auth1")
                        .build(),
                new Context(authUser),
                federatedStore);

        assertEquals(1, federatedStore.getGraphs(authUser, null).size());

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(schema)
                            .storeProperties(graphStoreProperties)
                            .graphAuths("nonMatchingAuth")
                            .build(),
                    new Context(testUser),
                    federatedStore);
            fail("exception expected");
        } catch (final OperationException e) {
            assertEquals(String.format("Error adding graph %s to storage due to: User is attempting to overwrite a graph within FederatedStore. GraphId: %s", EXPECTED_GRAPH_ID, EXPECTED_GRAPH_ID), e.getCause().getMessage());
            String message = "error message should not contain details about schema";
            assertFalse(message, e.getMessage().contains(unusualType));
            assertFalse(message, e.getMessage().contains(groupEdge));
            assertFalse(message, e.getMessage().contains(groupEnt));
        }

        assertTrue(federatedStore.getGraphs(testUser(), null).isEmpty());
    }
}