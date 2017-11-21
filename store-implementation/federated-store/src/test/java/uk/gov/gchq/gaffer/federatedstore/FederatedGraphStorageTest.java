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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.AUTH_2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.TEST_USER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.authUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.testUser;

public class FederatedGraphStorageTest {

    public static final String GRAPH_ID_A = "a";
    public static final String GRAPH_ID_B = "b";
    public static final String EXCEPTION_EXPECTED = "Exception expected";
    public static final String X = "x";
    private FederatedGraphStorage graphStorage;
    private AccumuloProperties accumuloProperties;
    private Graph a;
    private Graph b;
    private User nullUser;
    private User testUser;
    private User authUser;
    private User blankUser;
    private Context nullUserContext;
    private Context testUserContext;
    private Context authUserContext;
    private Context blankUserContext;
    private FederatedAccess access;
    private FederatedAccess altAccess;
    private SchemaEntityDefinition e1;
    private SchemaEntityDefinition e2;
    private static final String UNUSUAL_TYPE = "unusualType";
    private static final String GROUP_ENT = "ent";
    private static final String GROUP_EDGE = "edg";

    @Before
    public void setUp() throws Exception {
        graphStorage = new FederatedGraphStorage();
        e1 = new SchemaEntityDefinition.Builder()
                .vertex("string")
                .build();
        a = new Builder()
                .config(new GraphConfig(GRAPH_ID_A))
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema.Builder()
                        .entity("e1", e1)
                        .type("string", String.class)
                        .build())
                .build();

        e2 = new SchemaEntityDefinition.Builder()
                .vertex("string2")
                .build();
        accumuloProperties = new AccumuloProperties();
        accumuloProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        b = new Builder()
                .config(new GraphConfig(GRAPH_ID_B))
                .storeProperties(accumuloProperties)
                .addSchema(new Schema.Builder()
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .build();
        nullUser = null;
        testUser = testUser();
        authUser = authUser();
        blankUser = blankUser();
        nullUserContext = new Context(nullUser);
        testUserContext = new Context(testUser);
        authUserContext = new Context(authUser);
        blankUserContext = new Context(blankUser);

        access = new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER);
        altAccess = new FederatedAccess(Sets.newHashSet(AUTH_2), TEST_USER);
    }

    @Test
    public void shouldStartWithNoGraphs() throws Exception {
        final Collection<Graph> graphs = graphStorage.get(nullUser, null);
        assertEquals(0, graphs.size());
    }


    @Test
    public void shouldGetIdForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(testUser);
        assertEquals(1, allIds.size());
        assertEquals(GRAPH_ID_A, allIds.iterator().next());
    }

    @Test
    public void shouldGetIdForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(authUser);
        assertEquals(1, allIds.size());
        assertEquals(GRAPH_ID_A, allIds.iterator().next());
    }

    @Test
    public void shouldNotGetIdForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(blankUser);
        assertEquals(0, allIds.size());
        assertFalse(allIds.iterator().hasNext());
    }

    @Test
    public void shouldGetGraphForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(testUser);
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldGetGraphForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(authUser);
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldNotGetGraphForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(blankUser);
        assertEquals(0, allGraphs.size());
        assertFalse(allGraphs.iterator().hasNext());
    }

    @Test
    public void shouldGetGraphForAddingUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.get(testUser, Lists.newArrayList(GRAPH_ID_A));
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldGetGraphForAuthUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.get(authUser, Lists.newArrayList(GRAPH_ID_A));
        assertEquals(1, allGraphs.size());
        assertEquals(a, allGraphs.iterator().next());
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(blankUser, Lists.newArrayList(GRAPH_ID_A));
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(GRAPH_ID_A)), e.getMessage());
        }
    }

    @Test
    public void shouldNotGetGraphForAddingUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(testUser, Lists.newArrayList(X));
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)), e.getMessage());
        }
    }

    @Test
    public void shouldNotGetGraphForAuthUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(authUser, Lists.newArrayList(X));
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)), e.getMessage());
        }
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        try {
            graphStorage.get(blankUser, Lists.newArrayList(X));
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)), e.getMessage());
        }
    }

    @Test
    public void shouldSchemaShouldChangeWhenAddingGraphB() throws Exception {
        graphStorage.put(a, access);
        final Schema schemaA = graphStorage.getSchema((Map<String, String>) null, testUserContext);
        assertEquals(1, schemaA.getTypes().size());
        assertEquals(String.class, schemaA.getType("string").getClazz());
        assertEquals(e1, schemaA.getElement("e1"));
        graphStorage.put(b, access);
        final Schema schemaAB = graphStorage.getSchema((Map<String, String>) null, testUserContext);
        assertNotEquals(schemaA, schemaAB);
        assertEquals(2, schemaAB.getTypes().size());
        assertEquals(String.class, schemaAB.getType("string").getClazz());
        assertEquals(String.class, schemaAB.getType("string2").getClazz());
        assertEquals(e1, schemaAB.getElement("e1"));
        assertEquals(e2, schemaAB.getElement("e2"));
    }


    @Test
    public void shouldGetSchemaForAddingUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, testUserContext);
        assertNotEquals("Revealing hidden schema", 2, schema.getTypes().size());
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldGetSchemaForAuthUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, authUserContext);
        assertNotEquals("Revealing hidden schema", 2, schema.getTypes().size());
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldNotGetSchemaForBlankUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, blankUserContext);
        assertNotEquals("Revealing hidden schema", 2, schema.getTypes().size());
        assertEquals("Revealing hidden schema", 0, schema.getTypes().size());
    }

    @Test
    public void shouldRemoveForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser);
        assertTrue(remove);
    }

    @Test
    public void shouldRemoveForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, authUser);
        assertTrue(remove);
    }

    @Test
    public void shouldNotRemoveForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser);
        assertFalse(remove);
    }

    @Test
    public void shouldGetGraphsInOrder() throws Exception {
        // Given
        graphStorage.put(Lists.newArrayList(a, b), access);
        final List<String> configAB = Arrays.asList(a.getGraphId(), b.getGraphId());
        final List<String> configBA = Arrays.asList(b.getGraphId(), a.getGraphId());

        // When
        final Collection<Graph> graphsAB = graphStorage.get(authUser, configAB);
        final Collection<Graph> graphsBA = graphStorage.get(authUser, configBA);

        // Then
        // A B
        final Iterator<Graph> itrAB = graphsAB.iterator();
        assertSame(a, itrAB.next());
        assertSame(b, itrAB.next());
        assertFalse(itrAB.hasNext());
        // B A
        final Iterator<Graph> itrBA = graphsBA.iterator();
        assertSame(b, itrBA.next());
        assertSame(a, itrBA.next());
        assertFalse(itrBA.hasNext());
    }

    @Test
    public void shouldNotAddGraphWhenLibraryThrowsExceptionDuringAdd() throws Exception {
        //given
        GraphLibrary mock = Mockito.mock(GraphLibrary.class);
        String testMockException = "testMockException";
        String graphId = a.getGraphId();
        Mockito.doThrow(new RuntimeException(testMockException))
                .when(mock)
                .checkExisting(graphId, a.getSchema(), a.getStoreProperties());
        graphStorage.setGraphLibrary(mock);
        try {
            graphStorage.put(a, access);
            fail(EXCEPTION_EXPECTED);
        } catch (final Exception e) {
            assertTrue(e instanceof StorageException);
            assertEquals(testMockException, e.getCause().getMessage());
        }
        try {
            //when
            graphStorage.get(testUser, Lists.newArrayList(graphId));
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            //then
            assertEquals(String.format(GRAPH_IDS_NOT_VISIBLE, Arrays.toString(new String[]{graphId})), e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddingNullSchema() {
        // Given
        Graph nullGraph = null;

        // When / Then
        try {
            graphStorage.put(nullGraph, access);
        } catch (StorageException e) {
            assertEquals("Graph cannot be null", e.getMessage());
        }
    }

    @Test
    public void checkSchemaNotLeakedWhenOverwritingExistingGraph() throws Exception {
        // Given
        graphStorage.setGraphLibrary(Mockito.mock(GraphLibrary.class));
        final String unusualType = "unusualType";
        final String groupEnt = "ent";
        final String groupEdge = "edg";
        Schema schemaNotToBeExposed = new Schema.Builder()
                .type(unusualType, String.class)
                .entity(groupEnt, new SchemaEntityDefinition.Builder()
                        .vertex(unusualType)
                        .build())
                .edge(groupEdge, new SchemaEdgeDefinition.Builder()
                        .source(unusualType)
                        .destination(unusualType)
                        .build())
                .build();

        final Graph graph1 = new Graph.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .storeProperties(accumuloProperties)
                .addSchema(schemaNotToBeExposed)
                .build();
        graphStorage.put(graph1, access);

        final Graph graph2 = new Graph.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .addSchema(new Schema.Builder()
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .storeProperties(accumuloProperties)
                .build();

        // When / Then
        try {
            graphStorage.put(graph2, access);
            fail(EXCEPTION_EXPECTED);
        } catch (StorageException e) {
            assertEquals("Error adding graph " + GRAPH_ID_A + " to storage due to: " + String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_A), e.getMessage());
            testNotLeakingContents(e, unusualType, groupEdge, groupEnt);
        }
    }

    private void testNotLeakingContents(final StorageException e, final String... values) {
        String message = "error message should not contain details about schema";
        for (String value : values) {
            assertFalse(message, e.getMessage().contains(value));
        }
    }

    @Test
    public void checkSchemaNotLeakedWhenAlreadyExistsUnderDifferentAccess() throws Exception {
        // Given
        Schema schemaNotToBeExposed = new Schema.Builder()
                .type(UNUSUAL_TYPE, String.class)
                .entity(GROUP_ENT, new SchemaEntityDefinition.Builder()
                        .vertex(UNUSUAL_TYPE)
                        .build())
                .edge(GROUP_EDGE, new SchemaEdgeDefinition.Builder()
                        .source(UNUSUAL_TYPE)
                        .destination(UNUSUAL_TYPE)
                        .build())
                .build();


        final Graph graph1 = new Graph.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .storeProperties(accumuloProperties)
                .addSchema(schemaNotToBeExposed)
                .build();
        graphStorage.put(graph1, access);

        // When / Then
        try {
            graphStorage.put(graph1, altAccess);
        } catch (StorageException e) {
            assertEquals("Error adding graph " + GRAPH_ID_A + " to storage due to: " + String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_A), e.getMessage());
            testNotLeakingContents(e, UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);
        }
    }

    @Test
    public void checkSchemaNotLeakedWhenAlreadyExistsUnderDifferentAccessWithOtherGraphs() throws Exception {
        // Given
        final String unusualType = "unusualType";
        final String groupEnt = "ent";
        final String groupEdge = "edg";
        Schema schemaNotToBeExposed = new Schema.Builder()
                .type(unusualType, String.class)
                .entity(groupEnt, new SchemaEntityDefinition.Builder()
                        .vertex(unusualType)
                        .build())
                .edge(groupEdge, new SchemaEdgeDefinition.Builder()
                        .source(unusualType)
                        .destination(unusualType)
                        .build())
                .build();

        final Graph graph1 = new Graph.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .storeProperties(accumuloProperties)
                .addSchema(schemaNotToBeExposed)
                .build();
        graphStorage.put(graph1, access);

        final Graph graph2 = new Graph.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_B).build())
                .addSchema(new Schema.Builder()
                        .merge(schemaNotToBeExposed)
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .storeProperties(accumuloProperties)
                .build();
        graphStorage.put(graph2, altAccess);

        // When / Then
        try {
            graphStorage.put(graph2, access);
        } catch (StorageException e) {
            assertEquals("Error adding graph " + GRAPH_ID_B + " to storage due to: " + String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_B), e.getMessage());
            testNotLeakingContents(e, unusualType, groupEdge, groupEnt);
        }
    }
}
