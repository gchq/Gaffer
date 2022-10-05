/*
 * Copyright 2017-2022 Crown Copyright
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
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextAuthUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_2;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedGraphStorageTest {

    public static final String GRAPH_ID_A = GRAPH_ID_ACCUMULO + "A";
    public static final String GRAPH_ID_B = GRAPH_ID_ACCUMULO + "B";
    public static final String X = "x";
    private static final String UNUSUAL_TYPE = "unusualType";
    private static final String GROUP_ENT = ENTITIES + "Unusual";
    private static final String GROUP_EDGE = EDGES + "Unusual";
    private static final Set<String> NULL_GRAPH_AUTHS = null;
    private static final AccumuloProperties PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private final GraphSerialisable graphSerialisableA = getGraphSerialisable(GRAPH_ID_A, 1);
    private final GraphSerialisable graphSerialisableB = getGraphSerialisable(GRAPH_ID_B, 2);
    private final User nullUser = null;
    private final FederatedAccess altAuth2Access = new FederatedAccess(Sets.newHashSet(AUTH_2), TEST_USER_ID);
    private final FederatedAccess disabledByDefaultAccess = new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER_ID, false, true);
    private final AccessPredicate blockingAccessPredicate = new NoAccessPredicate();
    private final FederatedAccess blockingReadAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, blockingAccessPredicate, null);
    private final FederatedAccess blockingWriteAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, null, blockingAccessPredicate);
    private final AccessPredicate permissiveAccessPredicate = new UnrestrictedAccessPredicate();
    private final FederatedAccess permissiveReadAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, permissiveAccessPredicate, null);
    private final FederatedAccess permissiveWriteAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, null, permissiveAccessPredicate);
    private final FederatedAccess auth1Access = new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER_ID);
    private FederatedGraphStorage graphStorage;

    @BeforeEach
    public void setUp() throws Exception {
        graphStorage = new FederatedGraphStorage();
    }

    @Test
    public void shouldValidateAssumptionStartWithNoGraphs() throws Exception {
        //when
        final Collection<Graph> graphs = graphStorage.get(nullUser, null);
        //then
        assertThat(graphs).isEmpty();
    }


    @Test
    public void shouldGetIdForAddingUser() throws Exception {
        //given
        //access includes testUser
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(testUser());
        //then
        assertThat(allIds).containsExactly(GRAPH_ID_A);
    }

    @Test
    public void shouldNotGetIdForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(testUser());
        //then
        assertThat(allIds).isEmpty();
    }

    @Test
    public void shouldGetIdForDisabledGraphsByTheAddingUser() throws Exception {
        //given
        //access includes testUser
        graphStorage.put(graphSerialisableA, disabledByDefaultAccess);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(testUser());
        //then
        assertThat(allIds).containsExactly(GRAPH_ID_A);
    }

    @Test
    public void shouldGetIdForDisabledGraphsWhenAsked() throws Exception {
        //given
        //access includes testUser
        graphStorage.put(graphSerialisableA, disabledByDefaultAccess);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(testUser());
        //then
        assertThat(allIds).containsExactly(GRAPH_ID_A);
    }

    @Test
    public void shouldGetIdForAuthUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(authUser());
        //then
        assertThat(allIds).containsExactly(GRAPH_ID_A);
    }

    @Test
    public void shouldNotGetIdForBlankUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(blankUser());
        //then
        assertThat(allIds).isEmpty();
    }

    @Test
    public void shouldGetIdForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveReadAccess);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(blankUser());
        //then
        assertThat(allIds).containsExactly(GRAPH_ID_A);
    }

    @Test
    public void shouldGetGraphForAddingUser() throws Exception {
        //given
        //access contains adding user
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<Graph> allGraphs = graphStorage.getAll(testUser());
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotGetGraphForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        //when
        final Collection<Graph> allGraphs = graphStorage.getAll(testUser());
        //then
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldGetGraphForAuthUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<Graph> allGraphs = graphStorage.getAll(authUser());
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldGetDisabledGraphWhenGetAll() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, disabledByDefaultAccess);
        //when
        final Collection<Graph> allGraphs = graphStorage.getAll(authUser()); //TODO FS disabledByDefault: has auths but still getting the graph so when and why is it disabled?
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotGetGraphForBlankUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<Graph> allGraphs = graphStorage.getAll(blankUser());
        //then
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldGetGraphForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveReadAccess);
        //then
        final Collection<Graph> allGraphs = graphStorage.getAll(blankUser());
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldGetGraphForAddingUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<Graph> allGraphs = graphStorage.get(testUser(), Lists.newArrayList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotGetGraphForAddingUserWithCorrectIdWhenBlockingReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        //when
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(testUser(), Lists.newArrayList(GRAPH_ID_A)))
                .withMessageContaining(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(GRAPH_ID_A)));
    }

    @Test
    public void shouldGetGraphForAuthUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<Graph> allGraphs = graphStorage.get(authUser(), Lists.newArrayList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldGetDisabledGraphForAuthUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, disabledByDefaultAccess);
        //when
        final Collection<Graph> allGraphs = graphStorage.get(authUser(), Lists.newArrayList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotGetDisabledGraphForAuthUserWhenNoIdsProvided() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, disabledByDefaultAccess);
        //when
        final Collection<Graph> allGraphs = graphStorage.get(authUser(), null); //TODO FS disabledByDefault: why only missing when get(null)
        //then
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(blankUser(), Lists.newArrayList(GRAPH_ID_A)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(GRAPH_ID_A)));
    }

    @Test
    public void shouldGetGraphForBlankUserWithCorrectIdWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveReadAccess);
        //when
        final Collection<Graph> allGraphs = graphStorage.get(blankUser(), Lists.newArrayList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotGetGraphForAddingUserWithIncorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(testUser(), Lists.newArrayList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)));
    }

    @Test
    public void shouldNotGetGraphForAuthUserWithIncorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(authUser(), Lists.newArrayList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)));
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithIncorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(blankUser(), Lists.newArrayList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(X)));
    }

    @Test
    @Deprecated // TODO FS Port to FedSchema Tests, when getSchema is deleted
    public void shouldChangeSchemaWhenAddingGraphB() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        final Schema schemaA = graphStorage.getSchema(null, contextTestUser());
        assertEquals(1, schemaA.getTypes().size());
        assertEquals(String.class, schemaA.getType(STRING + 1).getClazz());
        assertEquals(getEntityDefinition(1), schemaA.getElement(ENTITIES + 1));
        graphStorage.put(graphSerialisableB, auth1Access);
        final Schema schemaAB = graphStorage.getSchema(null, contextTestUser());
        assertNotEquals(schemaA, schemaAB);
        assertEquals(2, schemaAB.getTypes().size());
        assertEquals(String.class, schemaAB.getType(STRING + 1).getClazz());
        assertEquals(String.class, schemaAB.getType(STRING + 2).getClazz());
        assertEquals(getEntityDefinition(1), schemaAB.getElement(ENTITIES + 1));
        assertEquals(getEntityDefinition(2), schemaAB.getElement(ENTITIES + 2));
    }


    @Test
    @Deprecated // TODO FS Port to FedSchema Tests, when getSchema is deleted
    public void shouldGetSchemaForAddingUser() throws Exception {
        graphStorage.put(graphSerialisableA, auth1Access);
        graphStorage.put(graphSerialisableB, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema(null, contextTestUser());
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType(STRING + 1).getClazz());
        assertEquals(getEntityDefinition(1), schema.getElement(ENTITIES + 1));
    }

    @Test
    @Deprecated // TODO FS Port to FedSchema Tests, when getSchema is deleted
    public void shouldNotGetSchemaForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        graphStorage.put(graphSerialisableB, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema(null, contextTestUser());
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(0, schema.getTypes().size(), "Revealing hidden schema");
    }

    @Test
    @Deprecated // TODO FS Port to FedSchema Tests, when getSchema is deleted
    public void shouldGetSchemaForAuthUser() throws Exception {
        graphStorage.put(graphSerialisableA, auth1Access);
        graphStorage.put(graphSerialisableB, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema(null, contextAuthUser());
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType(STRING + 1).getClazz());
        assertEquals(getEntityDefinition(1), schema.getElement(ENTITIES + 1));
    }

    @Test
    @Deprecated // TODO FS Port to FedSchema Tests, when getSchema is deleted
    public void shouldNotGetSchemaForBlankUser() throws Exception {
        graphStorage.put(graphSerialisableA, auth1Access);
        graphStorage.put(graphSerialisableB, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema(null, contextBlankUser());
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(0, schema.getTypes().size(), "Revealing hidden schema");
    }

    @Test
    @Deprecated // TODO FS Port to FedSchema Tests, when getSchema is deleted
    public void shouldGetSchemaForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        graphStorage.put(graphSerialisableA, permissiveReadAccess);
        graphStorage.put(graphSerialisableB, new FederatedAccess(Sets.newHashSet(X), X));
        final Schema schema = graphStorage.getSchema(null, contextBlankUser());
        assertNotEquals(2, schema.getTypes().size(), "Revealing hidden schema");
        assertEquals(1, schema.getTypes().size());
        assertEquals(String.class, schema.getType(STRING + 1).getClazz());
        assertEquals(getEntityDefinition(1), schema.getElement(ENTITIES + 1));
    }

    @Test
    public void shouldRemoveForAddingUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser());
        final Collection<Graph> graphs = graphStorage.getAll(testUser());
        //when
        assertTrue(remove);
        assertThat(graphs).isEmpty();
    }

    @Test
    public void shouldNotRemoveForAddingUserWhenBlockingWriteAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingWriteAccess);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser());
        final Collection<Graph> graphs = graphStorage.getAll(testUser());
        //then
        assertFalse(remove);
        assertThat(graphs).containsExactly(graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotRemoveForAuthUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, authUser());
        //then
        assertFalse(remove);
    }

    @Test
    public void shouldNotRemoveForBlankUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser());
        //then
        assertFalse(remove);
    }

    @Test
    public void shouldRemoveForBlankUserWhenPermissiveWriteAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveWriteAccess);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser());
        //then
        assertTrue(remove);
    }

    @Test
    public void shouldGetGraphsInOrder() throws Exception {
        // Given
        graphStorage.put(Lists.newArrayList(graphSerialisableA, graphSerialisableB), auth1Access);
        final List<String> configAB = Arrays.asList(GRAPH_ID_A, GRAPH_ID_B);
        final List<String> configBA = Arrays.asList(GRAPH_ID_B, GRAPH_ID_A);

        // When
        final Collection<Graph> graphsAB = graphStorage.get(authUser(), configAB);
        final Collection<Graph> graphsBA = graphStorage.get(authUser(), configBA);

        // Then
        // A B
        assertThat(graphsAB).containsExactly(graphSerialisableA.getGraph(), graphSerialisableB.getGraph());
        // B A
        assertThat(graphsBA).containsExactly(graphSerialisableB.getGraph(), graphSerialisableA.getGraph());
    }

    @Test
    public void shouldNotAddGraphWhenLibraryThrowsExceptionDuringAdd() throws Exception {
        //given
        GraphLibrary mock = mock(GraphLibrary.class);
        String testMockException = "testMockException";
        Mockito.doThrow(new RuntimeException(testMockException))
                .when(mock)
                .checkExisting(GRAPH_ID_A, graphSerialisableA.getDeserialisedSchema(), graphSerialisableA.getDeserialisedProperties());
        graphStorage.setGraphLibrary(mock);

        final StorageException storageException = assertThrows(StorageException.class, () -> graphStorage.put(graphSerialisableA, auth1Access));
        assertThat(storageException).message().contains(testMockException);

        final IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> graphStorage.get(testUser(), Lists.newArrayList(GRAPH_ID_A)));
        assertThat(illegalArgumentException).message().isEqualTo(String.format(GRAPH_IDS_NOT_VISIBLE, Arrays.toString(new String[]{GRAPH_ID_A})));
    }

    @Test
    public void shouldThrowExceptionWhenAddingNullSchema() {
        //given
        GraphSerialisable nullGraph = null;
        //when
        final StorageException storageException = assertThrows(StorageException.class, () -> graphStorage.put(nullGraph, auth1Access));
        //then
        assertThat(storageException).message().isEqualTo("Graph cannot be null");
    }

    @Test
    public void checkSchemaNotLeakedWhenOverwritingExistingGraph() throws Exception {
        // Given
        graphStorage.setGraphLibrary(mock(GraphLibrary.class));

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

        graphStorage.put(
                new GraphSerialisable.Builder()
                        .config(new GraphConfig(GRAPH_ID_A))
                        .properties(PROPERTIES.clone())
                        .schema(schemaNotToBeExposed)
                        .build(), auth1Access);

        // When / Then
        final StorageException storageException = assertThrows(StorageException.class, () ->
                graphStorage.put(
                        new GraphSerialisable.Builder()
                                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                                .schema(getSchema(2))
                                .properties(PROPERTIES.clone())
                                .build(), auth1Access));

        assertThat(storageException)
                .message()
                .isEqualTo("Error adding graph " + GRAPH_ID_A + " to storage due to: " + String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_A))
                .withFailMessage("error message should not contain details about schema")
                .doesNotContain(UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);
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


        final GraphSerialisable graph = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .properties(PROPERTIES.clone())
                .schema(schemaNotToBeExposed)
                .build();

        graphStorage.put(graph, auth1Access);

        // When / Then
        final StorageException storageException = assertThrows(StorageException.class, () -> graphStorage.put(graph, altAuth2Access));

        assertThat(storageException)
                .message()
                .isEqualTo("Error adding graph " + GRAPH_ID_A + " to storage due to: " + String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_A))
                .withFailMessage("error message should not contain details about schema")
                .doesNotContain(UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);
    }

    @Test
    public void checkSchemaNotLeakedWhenAlreadyExistsUnderDifferentAccessWithOtherGraphs() throws Exception {
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

        graphStorage.put(new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                .properties(PROPERTIES.clone())
                .schema(schemaNotToBeExposed)
                .build(), auth1Access);

        final GraphSerialisable graph2 = new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder().graphId(GRAPH_ID_B).build())
                .schema(new Schema.Builder()
                        .merge(schemaNotToBeExposed)
                        .entity(ENTITIES + 2, getEntityDefinition(2))
                        .type(STRING + 2, String.class)
                        .build())
                .properties(PROPERTIES.clone())
                .build();

        graphStorage.put(graph2, altAuth2Access);

        // When / Then
        final StorageException storageException = assertThrows(StorageException.class, () -> graphStorage.put(graph2, auth1Access));

        assertThat(storageException)
                .message()
                .isEqualTo("Error adding graph " + GRAPH_ID_B + " to storage due to: " + String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_B))
                .withFailMessage("error message should not contain details about schema")
                .doesNotContain(UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);

    }

    private GraphSerialisable getGraphSerialisable(final String graphId, int i) {
        return new GraphSerialisable.Builder()
                .config(new GraphConfig(graphId))
                .properties(PROPERTIES.clone())
                .schema(getSchema(i))
                .build();
    }

    private Schema getSchema(final int i) {
        return new Schema.Builder()
                .entity(ENTITIES + i, getEntityDefinition(i))
                .type(STRING + i, String.class)
                .build();
    }

    private SchemaEntityDefinition getEntityDefinition(final int i) {
        return new SchemaEntityDefinition.Builder()
                .vertex(STRING + i)
                .build();
    }
}
