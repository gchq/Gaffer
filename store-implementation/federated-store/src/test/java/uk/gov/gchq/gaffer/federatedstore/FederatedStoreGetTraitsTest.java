/*
 * Copyright 2021-2022 Crown Copyright
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionIntersect;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_MAP;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.MAP_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextAuthUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.MATCHED_VERTEX;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.QUERY_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.UNUSED_AUTH_STRING;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.nullUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreGetTraitsTest {
    public static final StoreProperties PROPERTIES_ACCUMULO_STORE_SINGLE_USE = loadStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    public static final StoreProperties PROPERTIES_MAP_STORE = loadStoreProperties(MAP_STORE_SINGLE_USE_PROPERTIES);
    private static final Set<StoreTrait> ACCUMULO_TRAITS_EXCLUSIVE_OF_MAP = ImmutableSet.of(
            STORE_VALIDATION,
            ORDERED);
    private static final Set<StoreTrait> INTERSECTION_TRAITS = ImmutableSet.of(
            QUERY_AGGREGATION,
            TRANSFORMATION,
            PRE_AGGREGATION_FILTERING,
            VISIBILITY,
            POST_TRANSFORMATION_FILTERING,
            INGEST_AGGREGATION,
            POST_AGGREGATION_FILTERING,
            MATCHED_VERTEX);
    private static final Set<StoreTrait> MAP_TRAITS_EXCLUSIVE_OF_ACCUMULO = Collections.emptySet();
    private static final FederatedAccess ACCESS_UNUSED_AUTH_AND_UNUSED_USER = new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING);
    private static final FederatedAccess ACCESS_UNUSED_AUTH_WITH_TEST_USER = new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), TEST_USER_ID);
    private static final Set<StoreTrait> MAP_TRAITS = ImmutableSet.of(
            INGEST_AGGREGATION,
            MATCHED_VERTEX,
            POST_AGGREGATION_FILTERING,
            POST_TRANSFORMATION_FILTERING,
            PRE_AGGREGATION_FILTERING,
            QUERY_AGGREGATION,
            TRANSFORMATION,
            VISIBILITY);
    private static final Set<StoreTrait> ACCUMULO_TRAITS = AccumuloStore.TRAITS;
    private static final Set<StoreTrait> ACC_CURRENT_TRAITS = ImmutableSet.of(
            INGEST_AGGREGATION,
            MATCHED_VERTEX,
            ORDERED, POST_AGGREGATION_FILTERING,
            POST_TRANSFORMATION_FILTERING,
            PRE_AGGREGATION_FILTERING,
            TRANSFORMATION);
    private static final Set<StoreTrait> MAP_CURRENT_TRAITS = ImmutableSet.of(
            INGEST_AGGREGATION,
            POST_TRANSFORMATION_FILTERING,
            TRANSFORMATION,
            POST_AGGREGATION_FILTERING,
            MATCHED_VERTEX,
            PRE_AGGREGATION_FILTERING);
    private static final Set<String> NULL_GRAPH_AUTHS = null;
    private final User nullUser = nullUser();
    private final User testUser = testUser();
    private final User authUser = authUser();
    private final User blankUser = blankUser();
    private final Context testUserContext = contextTestUser();
    private final Context authUserContext = contextAuthUser();
    private final Context blankUserContext = contextBlankUser();
    private GetTraits getTraits;
    private AccessPredicate blockingAccessPredicate;
    private AccessPredicate permissiveAccessPredicate;
    private GraphSerialisable accumuloGraphSerialised;
    private GraphSerialisable mapGraphSerialised;
    private FederatedStore federatedStore;

    @AfterAll
    public static void afterEach() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        resetForFederatedTests();
        federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, new FederatedStoreProperties());

        accumuloGraphSerialised = new GraphSerialisable.Builder()
                .config(new GraphConfig(GRAPH_ID_ACCUMULO))
                .properties(PROPERTIES_ACCUMULO_STORE_SINGLE_USE.clone())
                .schema(new Schema.Builder()
                        .entity(ENTITIES, new SchemaEntityDefinition.Builder()
                                .vertex(STRING)
                                .build())
                        .type(STRING, String.class)
                        .build())
                .build();

        mapGraphSerialised = new GraphSerialisable.Builder()
                .config(new GraphConfig(GRAPH_ID_MAP))
                .properties(PROPERTIES_MAP_STORE.clone())
                .schema(new Schema.Builder()
                        .edge(EDGES, new SchemaEdgeDefinition.Builder()
                                .source(STRING)
                                .destination(STRING)
                                .build())
                        .type(STRING, String.class)
                        .build())
                .build();

        blockingAccessPredicate = new NoAccessPredicate();
        permissiveAccessPredicate = new UnrestrictedAccessPredicate();

        getTraits = new GetTraits();
    }

    @Test
    public void shouldVerifyTestAssumptionsThatNoTraitsFound() throws Exception {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> federatedStore.execute(getTraits, new Context(nullUser)))
                .withMessage("User is required");
        assertThat(federatedStore.execute(getTraits, contextTestUser())).withFailMessage("FederatedStore is not starting empty for test user").isNull();
        assertThat(federatedStore.execute(getTraits, contextAuthUser())).withFailMessage("FederatedStore is not starting empty for auth user").isNull();
        assertThat(federatedStore.execute(getTraits, contextBlankUser())).withFailMessage("FederatedStore is not starting empty for blank user").isNull();
    }

    @Test
    public void shouldVerifyTestAssumptionsThatStoreTraitsNonCurrent() throws Exception {
        // given
        getTraits.setCurrentTraits(false);
        final Set<StoreTrait> mapTraits = mapGraphSerialised.getGraph().execute(getTraits, testUser);
        final Set<StoreTrait> accTraits = accumuloGraphSerialised.getGraph().execute(getTraits, testUser);

        // when
        final Set<StoreTrait> mapTraitsExclusive = mapTraits.stream().filter(t -> !accTraits.contains(t)).collect(Collectors.toSet());
        final Set<StoreTrait> accTraitsExclusive = accTraits.stream().filter(t -> !mapTraits.contains(t)).collect(Collectors.toSet());
        final Set<StoreTrait> intersectionTraits = accTraits.stream().filter(mapTraits::contains).collect(Collectors.toSet());

        // then
        assertThat(accTraits).withFailMessage("This store does not have AccumuloStore Traits").containsExactlyInAnyOrderElementsOf(ACCUMULO_TRAITS);
        assertThat(mapTraits).withFailMessage("This store does not have MapStore Traits").containsExactlyInAnyOrderElementsOf(MAP_TRAITS)
                .withFailMessage("Test stores cannot have same traits").isNotEqualTo(accTraits);

        assertThat(mapTraitsExclusive).withFailMessage("Expected traits exclusive to MapStore is different").containsExactlyInAnyOrderElementsOf(MAP_TRAITS_EXCLUSIVE_OF_ACCUMULO);
        assertThat(accTraitsExclusive).withFailMessage("Expected traits exclusive to AccumuloStore is different").containsExactlyInAnyOrderElementsOf(ACCUMULO_TRAITS_EXCLUSIVE_OF_MAP);
        assertThat(intersectionTraits).withFailMessage("Expected intersection of traits is different").containsExactlyInAnyOrderElementsOf(INTERSECTION_TRAITS);
    }

    @Test
    public void shouldVerifyAssumptionsStoreTraitsCurrent() throws Exception {
        // given
        getTraits.setCurrentTraits(true);
        final Set<StoreTrait> mapTraitsIsCurrent = mapGraphSerialised.getGraph().execute(getTraits, testUser);
        final Set<StoreTrait> accTraitsIsCurrent = accumuloGraphSerialised.getGraph().execute(getTraits, testUser);

        // when
        final Set<StoreTrait> mapTraitsIsCurrentExclusive = mapTraitsIsCurrent.stream().filter(t -> !accTraitsIsCurrent.contains(t)).collect(Collectors.toSet());
        final Set<StoreTrait> accTraitsIsCurrentExclusive = accTraitsIsCurrent.stream().filter(t -> !mapTraitsIsCurrent.contains(t)).collect(Collectors.toSet());
        final Set<StoreTrait> intersectionTraitsIsCurrent = accTraitsIsCurrent.stream().filter(mapTraitsIsCurrent::contains).collect(Collectors.toSet());
        final Set<StoreTrait> mapTraitsIsCurrentIsSubSetOfStoreTraits = mapTraitsIsCurrent.stream().filter(t -> !MAP_TRAITS.contains(t)).collect(Collectors.toSet());
        final Set<StoreTrait> accTraitsIsCurrentIsSubSetOfStoreTraits = accTraitsIsCurrent.stream().filter(t -> !ACCUMULO_TRAITS.contains(t)).collect(Collectors.toSet());

        // then
        assertThat(accTraitsIsCurrent).isNotEqualTo(ACCUMULO_TRAITS)
                .withFailMessage("Expected traits for the AccumuloStore 'Current schema' is different").containsExactlyInAnyOrderElementsOf(ACC_CURRENT_TRAITS);
        assertThat(mapTraitsIsCurrent).isNotEqualTo(MAP_TRAITS)
                .withFailMessage("Expected traits for the MapStore 'Current schema' is different").containsExactlyInAnyOrderElementsOf(MAP_CURRENT_TRAITS);

        assertThat(mapTraitsIsCurrentExclusive).withFailMessage("Expected traits exclusive to MapStore is different").isEmpty();

        assertThat(accTraitsIsCurrentExclusive).withFailMessage("Expected traits exclusive to AccumuloStore is different")
                .containsExactlyInAnyOrder(ORDERED);
        assertThat(intersectionTraitsIsCurrent).withFailMessage("Expected intersection traits is different")
                .containsExactlyInAnyOrder(
                        INGEST_AGGREGATION,
                        MATCHED_VERTEX,
                        PRE_AGGREGATION_FILTERING,
                        TRANSFORMATION,
                        POST_AGGREGATION_FILTERING,
                        POST_TRANSFORMATION_FILTERING);

        assertThat(mapTraitsIsCurrentIsSubSetOfStoreTraits).withFailMessage("The IsCurrent traits is not a subset of MapStore traits").isEmpty();
        assertThat(accTraitsIsCurrentIsSubSetOfStoreTraits).withFailMessage("The IsCurrent traits is not a subset of AccumuloStore traits").isEmpty();
    }

    @Test
    public void shouldGetNonCurrentTraitsForAddingUser() throws Exception {
        // given
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_AND_UNUSED_USER, accumuloGraphSerialised);
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_WITH_TEST_USER, mapGraphSerialised);
        getTraits.setCurrentTraits(false);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, testUserContext);
        // then
        assertThat(traits).withFailMessage("Returning AccumuloStore traits instead of MapStore").isNotEqualTo(ACCUMULO_TRAITS)
                .withFailMessage("Revealing some hidden traits from the AccumuloStore instead of only MapStore").doesNotContainAnyElementsOf(ACCUMULO_TRAITS_EXCLUSIVE_OF_MAP)
                .withFailMessage("did not return map traits").isEqualTo(MAP_TRAITS);
    }

    @Test
    public void shouldGetCurrentTraitsForAddingUser() throws Exception {
        // given
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_AND_UNUSED_USER, accumuloGraphSerialised);
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_WITH_TEST_USER, mapGraphSerialised);
        getTraits.setCurrentTraits(true);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, testUserContext);
        // then
        assertThat(traits).withFailMessage("Returning AccumuloStore traits instead of MapStore").isNotEqualTo(ACCUMULO_TRAITS)
                .withFailMessage("Revealing some hidden traits from the AccumuloStore instead of only MapStore").doesNotContainAnyElementsOf(ACCUMULO_TRAITS_EXCLUSIVE_OF_MAP)
                .withFailMessage("did not return map current traits").isEqualTo(MAP_CURRENT_TRAITS);
    }

    @Test
    public void shouldGetCurrentTraitsForAddingUserButSelectedGraphsOnly() throws Exception {
        // given
        final GraphSerialisable accumuloGraphSerialised2 = new GraphSerialisable.Builder()
                .graph(accumuloGraphSerialised.getGraph())
                .config(new GraphConfig(GRAPH_ID_ACCUMULO + 2))
                .build();

        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_AND_UNUSED_USER, accumuloGraphSerialised);
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_WITH_TEST_USER, accumuloGraphSerialised2);
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_WITH_TEST_USER, mapGraphSerialised);

        getTraits.setCurrentTraits(true);

        // when
        final Set<StoreTrait> traits = (Set<StoreTrait>) federatedStore.execute(new FederatedOperation.Builder()
                .op(getTraits)
                .mergeFunction(new CollectionIntersect<Object>())
                .graphIds(GRAPH_ID_MAP)
                .build(), testUserContext);
        // then
        assertThat(traits).withFailMessage("Returning AccumuloStore traits instead of MapStore").isNotEqualTo(ACCUMULO_TRAITS)
                .withFailMessage("Revealing some hidden traits from the AccumuloStore instead of only MapStore").doesNotContainAnyElementsOf(ACCUMULO_TRAITS_EXCLUSIVE_OF_MAP)
                .withFailMessage("did not return current map traits").isEqualTo(MAP_CURRENT_TRAITS);

    }

    @Test
    public void shouldGetNonCurrentTraitsForAddingUserButSelectedGraphsOnly() throws Exception {
        //given
        final GraphSerialisable accumuloGraphSerialised2 = new GraphSerialisable.Builder()
                .graph(accumuloGraphSerialised.getGraph())
                .config(new GraphConfig(GRAPH_ID_ACCUMULO + 2))
                .build();

        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_AND_UNUSED_USER, accumuloGraphSerialised);
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_WITH_TEST_USER, accumuloGraphSerialised2);
        federatedStore.addGraphs(ACCESS_UNUSED_AUTH_WITH_TEST_USER, mapGraphSerialised);
        getTraits.setCurrentTraits(false);
        //when
        final Set<StoreTrait> traits = (Set<StoreTrait>) federatedStore.execute(new FederatedOperation.Builder()
                .op(getTraits)
                .mergeFunction(new CollectionIntersect<Object>())
                .graphIds(GRAPH_ID_MAP)
                .build(), testUserContext);

        //then
        assertThat(traits).withFailMessage("Returning AccumuloStore traits instead of MapStore traits").isNotEqualTo(ACCUMULO_TRAITS)
                .withFailMessage("Revealing some hidden traits from the AccumuloStore instead of only MapStore").doesNotContainAnyElementsOf(ACCUMULO_TRAITS_EXCLUSIVE_OF_MAP)
                .withFailMessage("did not return map traits").isEqualTo(MAP_TRAITS);
    }

    /**
     * Note:
     * The blockingAccessPredicate will stop ALL access, including Admin.
     * The default federated Read/Write Access predicates are being overridden, here.
     * The predicate controls the logic of how Users and Auths are granted access.
     *
     * @throws Exception exception
     */
    @Test
    public void shouldNotGetTraitsForAddingUserWhenBlockingReadAccessPredicateConfigured() throws Exception {
        // given
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING), accumuloGraphSerialised);
        federatedStore.addGraphs(new FederatedAccess(NULL_GRAPH_AUTHS, TEST_USER_ID, false, false, blockingAccessPredicate, null), mapGraphSerialised);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, testUserContext);
        // then
        assertThat(traits).withFailMessage("Revealing hidden traits").isNull();
    }

    @Test
    public void shouldGetTraitsForAuthUser() throws Exception {
        // given
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING), accumuloGraphSerialised);
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(AUTH_1), testUser.getUserId()), mapGraphSerialised);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, authUserContext);
        // then
        assertThat(traits).isEqualTo(MAP_CURRENT_TRAITS);
    }

    @Test
    public void shouldNotGetTraitsForBlankUser() throws Exception {
        // given
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING), accumuloGraphSerialised);
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(AUTH_1), TEST_USER_ID), mapGraphSerialised);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, blankUserContext);
        // then
        assertThat(traits).withFailMessage("Revealing hidden traits").isNull();
    }

    @Test
    public void shouldNotGetTraitsForNonAuthUser() throws Exception {
        // given
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(AUTH_1), AUTH_USER_ID), accumuloGraphSerialised);
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(AUTH_1), AUTH_USER_ID), mapGraphSerialised);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, testUserContext);
        // then
        assertThat(traits).withFailMessage("Revealing hidden traits").isNull();
    }

    /**
     * Note:
     * The permissiveAccessPredicate will allow ALL access.
     * The default federated Read/Write Access predicates are being overridden, here.
     * The predicate controls the logic of how Users and Auths are granted access.
     *
     * @throws Exception exception
     */
    @Test
    public void shouldGetTraitsForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws Exception {
        // given
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING), accumuloGraphSerialised);
        federatedStore.addGraphs(new FederatedAccess(NULL_GRAPH_AUTHS, UNUSED_AUTH_STRING, false, false, permissiveAccessPredicate, null), mapGraphSerialised);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, blankUserContext);
        // then
        assertThat(traits).isEqualTo(MAP_CURRENT_TRAITS);
    }

    /**
     * Note:
     * FederatedStore is Acting like 1 graph (comprised of requested subgraphs),
     * so it can only support the traits shared by all the subgraphs.
     * Traits must return the Intersection of traits for graphs.
     *
     * @throws Exception exception
     */
    @Test
    public void shouldCombineTraitsToMin() throws Exception {
        // given
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING, true), accumuloGraphSerialised);
        federatedStore.addGraphs(new FederatedAccess(Sets.newHashSet(UNUSED_AUTH_STRING), UNUSED_AUTH_STRING, true), mapGraphSerialised);
        getTraits.setCurrentTraits(false);
        // when
        final Set<StoreTrait> traits = federatedStore.execute(getTraits, testUserContext);
        // then
        assertThat(traits).isEqualTo(INTERSECTION_TRAITS);
    }
}
