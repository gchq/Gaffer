/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.nio.file.Path;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreAuthTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";

    private final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
    private User testUser;
    private User authUser;
    private FederatedStore federatedStore;
    private FederatedStoreProperties federatedStoreProperties;
    private Schema schema;
    private Operation ignore;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @BeforeAll
    public static void setUpStore(@TempDir Path tempDir) {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @BeforeEach
    public void setUp() throws Exception {
        testUser = testUser();
        authUser = authUser();

        CacheServiceLoader.shutdown();
        federatedStore = new FederatedStore();

        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        schema = new Schema.Builder().build();

        ignore = new GetAllElements();
    }

    @Test
    public void shouldAddGraphWithAuth() throws Exception {
        federatedStore.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(schema)
                        .storeProperties(PROPERTIES)
                        .graphAuths("auth1")
                        .build(),
                new Context(testUser),
                federatedStore);

        Collection<Graph> graphs = federatedStore.getGraphs(authUser, null, ignore);

        assertEquals(1, graphs.size());
        Graph next = graphs.iterator().next();
        assertEquals(EXPECTED_GRAPH_ID, next.getGraphId());
        assertEquals(schema, next.getSchema());

        graphs = federatedStore.getGraphs(blankUser(), null, ignore);

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
                .type(DIRECTED_EITHER, Boolean.class)
                .entity(groupEnt, new SchemaEntityDefinition.Builder()
                        .vertex(unusualType)
                        .build())
                .edge(groupEdge, new SchemaEdgeDefinition.Builder()
                        .source(unusualType)
                        .destination(unusualType)
                        .directed(DIRECTED_EITHER)
                        .build())
                .build();

        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .schema(schema)
                        .storeProperties(PROPERTIES)
                        .graphAuths("auth1")
                        .build(),
                new Context(authUser),
                federatedStore);

        assertEquals(1, federatedStore.getGraphs(authUser, null, ignore).size());

        try {
            federatedAddGraphHandler.doOperation(
                    new AddGraph.Builder()
                            .graphId(EXPECTED_GRAPH_ID)
                            .schema(schema)
                            .storeProperties(PROPERTIES)
                            .graphAuths("nonMatchingAuth")
                            .build(),
                    new Context(testUser),
                    federatedStore);
            fail("exception expected");
        } catch (final OperationException e) {
            assertEquals(String.format("Error adding graph %s to storage due to: User is attempting to overwrite a graph within FederatedStore. GraphId: %s", EXPECTED_GRAPH_ID, EXPECTED_GRAPH_ID), e.getCause().getMessage());
            String message = "error message should not contain details about schema";
            assertFalse(e.getMessage().contains(unusualType), message);
            assertFalse(e.getMessage().contains(groupEdge), message);
            assertFalse(e.getMessage().contains(groupEnt), message);
        }

        assertTrue(federatedStore.getGraphs(testUser(), null, ignore).isEmpty());
    }
}
