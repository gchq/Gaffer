/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICacheService;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
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

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreCacheTransient.getCacheNameFrom;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties.CACHE_SERVICE_CLASS_DEFAULT;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
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
    public static final String CACHE_NAME_SUFFIX = "suffix";
    private final GraphSerialisable graphSerialisableA = getGraphSerialisable(GRAPH_ID_A, 1);
    private final GraphSerialisable graphSerialisableB = getGraphSerialisable(GRAPH_ID_B, 2);
    private final User nullUser = null;
    private final FederatedAccess altAuth2Access = new FederatedAccess(singleton(AUTH_2), TEST_USER_ID);
    private final AccessPredicate blockingAccessPredicate = new NoAccessPredicate();
    private final FederatedAccess blockingReadAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, blockingAccessPredicate, null);
    private final FederatedAccess blockingWriteAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, null, blockingAccessPredicate);
    private final AccessPredicate permissiveAccessPredicate = new UnrestrictedAccessPredicate();
    private final FederatedAccess permissiveReadAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, permissiveAccessPredicate, null);
    private final FederatedAccess permissiveWriteAccess = new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, null, permissiveAccessPredicate);
    private final FederatedAccess auth1Access = new FederatedAccess(singleton(AUTH_1), TEST_USER_ID);
    private FederatedGraphStorage graphStorage;

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        CacheServiceLoader.initialise(CACHE_SERVICE_CLASS_DEFAULT);
        graphStorage = new FederatedGraphStorage(CACHE_NAME_SUFFIX);
    }

    @Test
    public void shouldValidateAssumptionStartWithNoGraphs() throws Exception {
        //when
        final Collection<GraphSerialisable> graphs = graphStorage.get(nullUser, null);
        //then
        assertThat(graphs).isEmpty();
    }


    @Test
    public void shouldGetIdForOwningUser() throws Exception {
        //given
        //access includes testUser
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(testUser());
        //then
        assertThat(allIds).containsExactly(GRAPH_ID_A);
    }

    @Test
    public void shouldNotGetIdForOwningUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        //when
        final Collection<String> allIds = graphStorage.getAllIds(testUser());
        //then
        assertThat(allIds).isEmpty();
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
    public void shouldGetGraphForOwningUser() throws Exception {
        //given
        //access contains adding user
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.getAll(testUser());
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldNotGetGraphForOwningUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.getAll(testUser());
        //then
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldGetGraphForAuthUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.getAll(authUser());
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldNotGetGraphForBlankUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.getAll(blankUser());
        //then
        assertThat(allGraphs).isEmpty();
    }

    @Test
    public void shouldGetGraphForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveReadAccess);
        //then
        final Collection<GraphSerialisable> allGraphs = graphStorage.getAll(blankUser());
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldGetGraphForOwningUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.get(testUser(), singletonList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldNotGetGraphForOwningUserWithCorrectIdWhenBlockingReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingReadAccess);
        //when
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(testUser(), singletonList(GRAPH_ID_A)))
                .withMessageContaining(String.format(GRAPH_IDS_NOT_VISIBLE, singleton(GRAPH_ID_A)));
    }

    @Test
    public void shouldGetGraphForAuthUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.get(authUser(), singletonList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithCorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(blankUser(), singletonList(GRAPH_ID_A)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, singleton(GRAPH_ID_A)));
    }

    @Test
    public void shouldGetGraphForBlankUserWithCorrectIdWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveReadAccess);
        //when
        final Collection<GraphSerialisable> allGraphs = graphStorage.get(blankUser(), singletonList(GRAPH_ID_A));
        //then
        assertThat(allGraphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldNotGetGraphForOwningUserWithIncorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(testUser(), singletonList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, singleton(X)));
    }

    @Test
    public void shouldNotGetGraphForAuthUserWithIncorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(authUser(), singletonList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, singleton(X)));
    }

    @Test
    public void shouldNotGetGraphForBlankUserWithIncorrectId() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        assertThatIllegalArgumentException()
                .isThrownBy(() -> graphStorage.get(blankUser(), singletonList(X)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, singleton(X)));
    }

    @Test
    public void shouldRemoveForOwningUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser(), false);
        final Collection<GraphSerialisable> graphs = graphStorage.getAll(testUser());
        //when
        assertThat(remove).isTrue();
        assertThat(graphs).isEmpty();
    }

    @Test
    public void shouldNotRemoveForOwningUserWhenBlockingWriteAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, blockingWriteAccess);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, testUser(), false);
        final Collection<GraphSerialisable> graphs = graphStorage.getAll(testUser());
        //then
        assertThat(remove).isFalse();
        assertThat(graphs).containsExactly(graphSerialisableA);
    }

    @Test
    public void shouldNotRemoveForAuthUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, authUser(), false);
        //then
        assertThat(remove).isFalse();
    }

    @Test
    public void shouldNotRemoveForBlankUser() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, auth1Access);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser(), false);
        //then
        assertThat(remove).isFalse();
    }

    @Test
    public void shouldRemoveForBlankUserWhenPermissiveWriteAccessPredicateConfigured() throws Exception {
        //given
        graphStorage.put(graphSerialisableA, permissiveWriteAccess);
        //when
        final boolean remove = graphStorage.remove(GRAPH_ID_A, blankUser(), false);
        //then
        assertThat(remove).isTrue();
    }

    @Test
    public void shouldGetGraphsInOrder() throws Exception {
        // Given
        graphStorage.put(Arrays.asList(graphSerialisableA, graphSerialisableB), auth1Access);
        final List<String> configAB = Arrays.asList(GRAPH_ID_A, GRAPH_ID_B);
        final List<String> configBA = Arrays.asList(GRAPH_ID_B, GRAPH_ID_A);

        // When
        final Collection<GraphSerialisable> graphsAB = graphStorage.get(authUser(), configAB);
        final Collection<GraphSerialisable> graphsBA = graphStorage.get(authUser(), configBA);

        // Then
        // A B
        assertThat(graphsAB).containsExactly(graphSerialisableA, graphSerialisableB);
        // B A
        assertThat(graphsBA).containsExactly(graphSerialisableB, graphSerialisableA);
    }

    @Test
    public void shouldNotAddGraphWhenLibraryThrowsExceptionDuringAdd() throws Exception {
        //given
        GraphLibrary mock = mock(GraphLibrary.class);
        String testMockException = "testMockException";
        Mockito.doThrow(new RuntimeException(testMockException))
                .when(mock)
                .checkExisting(GRAPH_ID_A, graphSerialisableA.getSchema(), graphSerialisableA.getStoreProperties());
        graphStorage.setGraphLibrary(mock);

        //then
        assertThatExceptionOfType(StorageException.class)
                .isThrownBy(() -> graphStorage.put(graphSerialisableA, auth1Access))
                .withMessageContaining(testMockException);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> graphStorage.get(testUser(), singletonList(GRAPH_ID_A)))
                .withMessage(String.format(GRAPH_IDS_NOT_VISIBLE, Arrays.toString(new String[]{GRAPH_ID_A})));
    }

    @Test
    public void shouldThrowExceptionWhenAddingNullSchema() {
        //given
        GraphSerialisable nullGraph = null;
        //then
        assertThatExceptionOfType(StorageException.class).isThrownBy(() -> graphStorage.put(nullGraph, auth1Access))
                .withMessage("Graph cannot be null");
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
        assertThatExceptionOfType(StorageException.class).isThrownBy(() ->
                        graphStorage.put(
                                new GraphSerialisable.Builder()
                                        .config(new GraphConfig.Builder().graphId(GRAPH_ID_A).build())
                                        .schema(getSchema(2))
                                        .properties(PROPERTIES.clone())
                                        .build(), auth1Access))


                .withMessageContaining("Error adding graph " + GRAPH_ID_A + " to storage due to: ")
                .withMessageContaining(String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_A))
                .withFailMessage("error message should not contain details about schema")
                .withMessageNotContainingAny(UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);
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
        assertThatExceptionOfType(StorageException.class)
                .isThrownBy(() -> graphStorage.put(graph, altAuth2Access))
                .withMessageContaining("Error adding graph " + GRAPH_ID_A + " to storage due to: ")
                .withMessageContaining(String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_A))
                .withFailMessage("error message should not contain details about schema")
                .withMessageNotContainingAny(UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);
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
        assertThatExceptionOfType(StorageException.class)
                .isThrownBy(() -> graphStorage.put(graph2, auth1Access))
                .withMessageContaining("Error adding graph " + GRAPH_ID_B + " to storage due to: ")
                .withMessageContaining(String.format(FederatedGraphStorage.USER_IS_ATTEMPTING_TO_OVERWRITE, GRAPH_ID_B))
                .withFailMessage("error message should not contain details about schema")
                .withMessageNotContainingAny(UNUSUAL_TYPE, GROUP_EDGE, GROUP_ENT);

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

    @Test
    public void shouldAddGraphWithCacheEnabled() throws StorageException {
        //given
        final ICacheService cacheService = CacheServiceLoader.getDefaultService();

        //when
        graphStorage.put(graphSerialisableA, auth1Access);
        final Collection<String> allIds = graphStorage.getAllIds(authUser());

        //then
        assertThat(cacheService.getCache(getCacheNameFrom(CACHE_NAME_SUFFIX)).getAllValues()).hasSize(1);
        assertThat(allIds).hasSize(1);
        assertThat(allIds.iterator().next()).isEqualTo(GRAPH_ID_A);
    }

    @Test
    public void shouldAddGraphReplicatedBetweenInstances() throws StorageException {
        //given
        final ICacheService cacheService = CacheServiceLoader.getDefaultService();
        final FederatedGraphStorage otherGraphStorage = new FederatedGraphStorage(CACHE_NAME_SUFFIX);

        //when
        otherGraphStorage.put(graphSerialisableA, auth1Access);
        final Collection<String> allIds = graphStorage.getAllIds(authUser());

        //then
        assertThat(cacheService.getCache(getCacheNameFrom(CACHE_NAME_SUFFIX)).getAllValues()).hasSize(1);
        assertThat(allIds).hasSize(1);
        assertThat(allIds.iterator().next()).isEqualTo(GRAPH_ID_A);
    }

}
