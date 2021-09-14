/*
 * Copyright 2017-2021 Crown Copyright
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_2;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedGraphStorageTest {

    public static final String GRAPH_ID_A = "a";
    public static final String GRAPH_ID_B = "b";
    public static final String EXCEPTION_EXPECTED = "Exception expected";
    public static final String X = "x";
    private AccessPredicate blockingAccessPredicate;
    private AccessPredicate permissiveAccessPredicate;
    private FederatedGraphStorage graphStorage;
    private GraphSerialisable a;
    private GraphSerialisable b;
    private User nullUser;
    private User testUser;
    private User authUser;
    private User blankUser;
    private Context testUserContext;
    private Context authUserContext;
    private Context blankUserContext;
    private FederatedAccess access;
    private FederatedAccess altAccess;
    private FederatedAccess disabledByDefaultAccess;
    private FederatedAccess blockingReadAccess;
    private FederatedAccess blockingWriteAccess;
    private FederatedAccess permissiveReadAccess;
    private FederatedAccess permissiveWriteAccess;
    private SchemaEntityDefinition e1;
    private SchemaEntityDefinition e2;
    private static final String UNUSUAL_TYPE = "unusualType";
    private static final String GROUP_ENT = "ent";
    private static final String GROUP_EDGE = "edg";
    private static final Set<String> NULL_GRAPH_AUTHS = null;

    private static Class currentClass = new Object() {
    }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));

    @BeforeEach
    public void setUp() throws Exception {
        graphStorage = new FederatedGraphStorage();

        e1 = new SchemaEntityDefinition.Builder()
                .vertex("string")
                .build();

        a = new GraphSerialisable.Builder()
                .config(new GraphConfig(GRAPH_ID_A))
                .properties(PROPERTIES)
                .schema(new Schema.Builder()
                        .entity("e1", e1)
                        .type("string", String.class)
                        .build())
                .build();

        e2 = new SchemaEntityDefinition.Builder()
                .vertex("string2")
                .build();

        b = new GraphSerialisable.Builder()
                .config(new GraphConfig(GRAPH_ID_B))
                .properties(PROPERTIES)
                .schema(new Schema.Builder()
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .build();

        nullUser = null;
        testUser = testUser();
        authUser = authUser();
        blankUser = blankUser();
        testUserContext = new Context(testUser);
        authUserContext = new Context(authUser);
        blankUserContext = new Context(blankUser);

        access = new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER_ID);
        altAccess = new FederatedAccess(Sets.newHashSet(AUTH_2), TEST_USER_ID);

        disabledByDefaultAccess = new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER_ID, false, true);

        blockingAccessPredicate = new NoAccessPredicate();
        blockingReadAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, blockingAccessPredicate, null);
        blockingWriteAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, null, blockingAccessPredicate);

        permissiveAccessPredicate = new UnrestrictedAccessPredicate();
        permissiveReadAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, permissiveAccessPredicate, null);
        permissiveWriteAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, null, permissiveAccessPredicate);
    }

    @Test
    public void shouldStartWithNoGraphs() throws Exception {
        final Collection<Graph> graphs = graphStorage.get(nullUser, null);
        assertThat(graphs).isEmpty();
    }


    @Test
    public void shouldGetIdForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(testUser);
        assertThat(allIds).hasSize(1);
        assertThat(allIds.iterator().next()).isEqualTo(GRAPH_ID_A);
    }

    @Test
    public void shouldNotGetIdForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, blockingReadAccess);
        final Collection<String> allIds = graphStorage.getAllIds(testUser);
        assertThat(allIds).isEmpty();
    }

    @Test
    public void shouldGetIdForDisabledGraphs() throws Exception {
        graphStorage.put(a, disabledByDefaultAccess);
        final Collection<String> allIds = graphStorage.getAllIds(testUser);
        assertThat(allIds).hasSize(1);
        assertThat(allIds.iterator().next()).isEqualTo(GRAPH_ID_A);
    }

    @Test
    public void shouldGetIdForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(authUser);
        assertThat(allIds).hasSize(1);
        assertThat(allIds.iterator().next()).isEqualTo(GRAPH_ID_A);
    }

    @Test
    public void shouldNotGetIdForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<String> allIds = graphStorage.getAllIds(blankUser);
        assertThat(allIds).isEmpty();
        assertFalse(allIds.iterator().hasNext());
    }

    @Test
    public void shouldGetIdForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, permissiveReadAccess);
        final Collection<String> allIds = graphStorage.getAllIds(blankUser);
        assertThat(allIds).hasSize(1);
        assertThat(allIds.iterator().next()).isEqualTo(GRAPH_ID_A);
    }

    @Test
    public void shouldGetGraphForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(testUser);
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldNotGetGraphForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, blockingReadAccess);
        final Collection<Graph> allGraphs = graphStorage.getAll(testUser);
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldGetGraphForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(authUser);
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldGetDisabledGraphWhenGetAll() throws Exception {
        graphStorage.put(a, disabledByDefaultAccess);
        final Collection<Graph> allGraphs = graphStorage.getAll(authUser);
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldNotGetGraphForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.getAll(blankUser);
        assertThat(allGraphs).isEmpty();
        assertFalse(allGraphs.iterator().hasNext());
    }

    @Test
    public void shouldGetGraphForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, permissiveReadAccess);
        final Collection<Graph> allGraphs = graphStorage.getAll(blankUser);
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldGetGraphForAddingUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.get(testUser, Lists.newArrayList(GRAPH_ID_A));
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldNotGetGraphForAddingUserWithCorrectIdWhenBlockingReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, blockingReadAccess);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(testUser, Lists.newArrayList(GRAPH_ID_A)))
                .withMessageContaining(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(GRAPH_ID_A)));
    }

    @Test
    public void shouldGetGraphForAuthUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        final Collection<Graph> allGraphs = graphStorage.get(authUser, Lists.newArrayList(GRAPH_ID_A));
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldGetDisabledGraphForAuthUserWithCorrectId() throws Exception {
        graphStorage.put(a, disabledByDefaultAccess);
        final Collection<Graph> allGraphs = graphStorage.get(authUser, Lists.newArrayList(GRAPH_ID_A));
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldNotGetDisabledGraphForAuthUserWhenNoIdsProvided() throws Exception {
        graphStorage.put(a, disabledByDefaultAccess);
        final Collection<Graph> allGraphs = graphStorage.get(authUser, null);
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithCorrectId() throws Exception {
        graphStorage.put(a, access);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(blankUser, Lists.newArrayList(GRAPH_ID_A)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(GRAPH_ID_A)));
    }

    @Test
    public void shouldGetGraphForBlankUserWithCorrectIdWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, permissiveReadAccess);
        final Collection<Graph> allGraphs = graphStorage.get(blankUser, Lists.newArrayList(GRAPH_ID_A));
        assertThat(allGraphs).hasSize(1);
        assertThat(allGraphs.iterator().next()).isEqualTo(a.getGraph());
    }

    @Test
    public void shouldNotGetGraphForAddingUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(testUser, Lists.newArrayList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)));
    }

    @Test
    public void shouldNotGetGraphForAuthUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(authUser, Lists.newArrayList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)));
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithIncorrectId() throws Exception {
        graphStorage.put(a, access);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(blankUser, Lists.newArrayList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)));
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
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldNotGetSchemaForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, blockingReadAccess);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, testUserContext);
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(0, schema.getTypes().size(), "Revealing hidden schema");
    }

    @Test
    public void shouldGetSchemaForAuthUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, authUserContext);
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldNotGetSchemaForBlankUser() throws Exception {
        graphStorage.put(a, access);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, blankUserContext);
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(0, schema.getTypes().size(), "Revealing hidden schema");
    }

    @Test
    public void shouldGetSchemaForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, permissiveReadAccess);
        graphStorage.put(b, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema((Map<String, String>) null, blankUserContext);
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType("string").getClazz());
        assertEquals(e1, schema.getElement("e1"));
    }

    @Test
    public void shouldRemoveForAddingUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser);
        assertTrue(remove);
    }

    @Test
    public void shouldNotRemoveForAddingUserWhenBlockingWriteAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, blockingWriteAccess);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser);
        assertFalse(remove);
    }

    @Test
    public void shouldNotRemoveForAuthUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, authUser);
        assertFalse(remove);
    }

    @Test
    public void shouldNotRemoveForBlankUser() throws Exception {
        graphStorage.put(a, access);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser);
        assertFalse(remove);
    }

    @Test
    public void shouldRemoveForBlankUserWhenPermissiveWriteAccessPredicateConfigured() throws Exception {
        graphStorage.put(a, permissiveWriteAccess);
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser);
        assertTrue(remove);
    }

    @Test
    public void shouldGetGraphsInOrder() throws Exception {
        // Given
        graphStorage.put(Lists.newArrayList(a, b), access);
        final List<String> configAB = Arrays.asList(a.getDeserialisedConfig().getGraphId(), b.getDeserialisedConfig().getGraphId());
        final List<String> configBA = Arrays.asList(b.getDeserialisedConfig().getGraphId(), a.getDeserialisedConfig().getGraphId());

        // When
        final Collection<Graph> graphsAB = graphStorage.get(authUser, configAB);
        final Collection<Graph> graphsBA = graphStorage.get(authUser, configBA);

        // Then
        // A B
        final Iterator<Graph> itrAB = graphsAB.iterator();
        assertSame(a.getGraph(), itrAB.next());
        assertSame(b.getGraph(), itrAB.next());
        assertThat(itrAB).isExhausted();
        // B A
        final Iterator<Graph> itrBA = graphsBA.iterator();
        assertSame(b.getGraph(), itrBA.next());
        assertSame(a.getGraph(), itrBA.next());
        assertThat(itrBA).isExhausted();
    }

    @Test
    public void shouldNotAddGraphWhenLibraryThrowsExceptionDuringAdd() throws Exception {
        //given
        GraphLibrary mock = mock(GraphLibrary.class);
        String testMockException = "testMockException";
        String graphId = a.getDeserialisedConfig().getGraphId();
        Mockito.doThrow(new RuntimeException(testMockException))
                .when(mock)
                .checkExisting(graphId, a.getDeserialisedSchema(), a.getDeserialisedProperties());
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
        GraphSerialisable nullGraph = null;

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
        graphStorage.setGraphLibrary(mock(GraphLibrary.class));
        final String unusualType = "unusualType";
        final String groupEnt = "ent";
        final String groupEdge = "edg";
        Schema schemaNotToBeExposed = new Schema.Builder()
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

        final GraphSerialisable graph1 = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .properties(PROPERTIES)
                .schema(schemaNotToBeExposed)
                .build();
        graphStorage.put(graph1, access);

        final GraphSerialisable graph2 = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .schema(new Schema.Builder()
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .properties(PROPERTIES)
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
            assertFalse(e.getMessage().contains(value), message);
        }
    }

    @Test
    public void checkSchemaNotLeakedWhenAlreadyExistsUnderDifferentAccess() throws Exception {
        // Given
        Schema schemaNotToBeExposed = new Schema.Builder()
                .type(UNUSUAL_TYPE, String.class)
                .type(DIRECTED_EITHER, Boolean.class)
                .entity(GROUP_ENT, new SchemaEntityDefinition.Builder()
                        .vertex(UNUSUAL_TYPE)
                        .build())
                .edge(GROUP_EDGE, new SchemaEdgeDefinition.Builder()
                        .source(UNUSUAL_TYPE)
                        .destination(UNUSUAL_TYPE)
                        .directed(DIRECTED_EITHER)
                        .build())
                .build();


        final GraphSerialisable graph1 = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .properties(PROPERTIES)
                .schema(schemaNotToBeExposed)
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

        final GraphSerialisable graph1 = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .properties(PROPERTIES)
                .schema(schemaNotToBeExposed)
                .build();
        graphStorage.put(graph1, access);

        final GraphSerialisable graph2 = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_B).build())
                .schema(new Schema.Builder()
                        .merge(schemaNotToBeExposed)
                        .entity("e2", e2)
                        .type("string2", String.class)
                        .build())
                .properties(PROPERTIES)
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
