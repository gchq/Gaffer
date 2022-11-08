/*
 * Copyright 2022 Crown Copyright
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SOURCE_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.basicEntitySchema;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.DEPRECATED_GRAPHIDS_OPTION;

public class FederatedStoreGraphIDsTest {

    public static final Entity EXPECTED_ENTITY = entityBasic();
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private FederatedStore federatedStore;

    @AfterAll
    public static void after() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();

        FederatedStoreProperties fedProps = new FederatedStoreProperties();
        fedProps.setCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, fedProps);

        // Setup graph A and graph B with same schema
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true).build(), contextBlankUser());

        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_B)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true).build(), contextBlankUser());

        // Add same entity to both
        federatedStore.execute(
                new AddElements.Builder()
                        .input(EXPECTED_ENTITY)
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                        .build(), contextBlankUser());

        federatedStore.execute(
                new AddElements.Builder()
                        .input(EXPECTED_ENTITY)
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_B)
                        .build(), contextBlankUser());
    }

    @Test
    public void shouldAggregateAcrossBothGraphsWhenNoGraphIdSelected() throws Exception {
        // When
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetElements.Builder()
                                .input(EXPECTED_ENTITY)
                                .build())
                        .then(new Limit<>(10))
                        .build(), contextBlankUser());

        // Then
        assertThat(results).hasSize(1);
        assertThat(results.iterator().next().getProperties()).containsEntry(PROPERTY_1, 2);
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOptionWithinOperationChain() throws Exception {
        // When
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetElements.Builder()
                                .input(EXPECTED_ENTITY)
                                .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                                .build())
                        .then(new Limit<>(10))
                        .build(), contextBlankUser());

        // Then
        assertThat(results).hasSize(1);
        assertThat(results.iterator().next().getProperties()).containsEntry(PROPERTY_1, 1);
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOptionWithinNestedOperationChain() throws Exception {
        // When
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new OperationChain.Builder()
                                .first(new GetElements.Builder()
                                    .input(EXPECTED_ENTITY)
                                    .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                                    .build())
                                .build())
                        .then(new Limit<>(10))
                        .build(), contextBlankUser());

        // Then
        assertThat(results).hasSize(1);
        assertThat(results.iterator().next().getProperties()).containsEntry(PROPERTY_1, 1);
    }

    @Test
    public void shouldSelectGraphUsingFederatedOperation() throws Exception {
        // When
        Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(
                new FederatedOperation.Builder()
                        .op(new OperationChain.Builder()
                                .first(new OperationChain.Builder()
                                        .first(new GetElements.Builder()
                                                .input(EXPECTED_ENTITY)
                                                .build())
                                        .build())
                                .then(new Limit<>(10))
                                .build())
                        .graphIdsCSV(GRAPH_ID_A)
                        .build(), contextBlankUser());

        // Then
        assertThat(results).hasSize(1);
        assertThat(results.iterator().next().getProperties()).containsEntry(PROPERTY_1, 1);
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOptionWithinLongOperationChain() throws Exception {
        // Given
        final Entity secondEntity = new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(SOURCE_BASIC)
                .property(PROPERTY_1, 2)
                .build();
        final Entity thirdEntity = new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(SOURCE_BASIC)
                .property(PROPERTY_1, 3)
                .build();

        federatedStore.execute(
                new AddElements.Builder()
                        .input(secondEntity)
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                        .build(), contextBlankUser());
        federatedStore.execute(
                new AddElements.Builder()
                        .input(thirdEntity)
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_B)
                        .build(), contextBlankUser());

        // When
        Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetElements.Builder()
                                .input(BASIC_VERTEX)
                                .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                                .build())
                        .then(new ExportToSet.Builder<>()
                                .key("results")
                                .build())
                        .then(new DiscardOutput())
                        .then(new GetElements.Builder()
                                .input(SOURCE_BASIC)
                                .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_B)
                                .build())
                        .then(new ExportToSet.Builder<>()
                                .key("results")
                                .build())
                        .then(new DiscardOutput())
                        .then(new GetSetExport.Builder()
                                .key("results")
                                .build())
                        .build(), contextBlankUser());

        // Then
        assertThat(results).hasSize(2);
        assertThat((Iterable<Entity>) results).containsExactlyInAnyOrder(EXPECTED_ENTITY, thirdEntity);
    }

    @Test
    public void shouldNotCorrectlySelectGraphUsingGraphIdsOptionOnOperationChain() throws Exception {
        // When
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetElements.Builder()
                                .input(EXPECTED_ENTITY)
                                .build())
                        .then(new Limit<>(10))
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                        .build(), contextBlankUser());

        // Then
        // Pre Gaffer 2.0 this would have sent the whole chain to graph A
        // Now, the chain's graphIds option is ignored as every individual
        // operation is wrapped in a FederatedOperation
        assertThat(results).hasSize(1);
        assertThat(results.iterator().next().getProperties()).containsEntry(PROPERTY_1, 2);
    }
}
