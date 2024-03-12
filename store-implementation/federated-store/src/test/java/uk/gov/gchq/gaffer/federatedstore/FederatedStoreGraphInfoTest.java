/*
 * Copyright 2022-2024 Crown Copyright
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
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_C;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.basicEntitySchema;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.DEPRECATED_GRAPHIDS_OPTION;

public class FederatedStoreGraphInfoTest {
    private FederatedStore federatedStore;

    @AfterAll
    public static void after() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, new FederatedStoreProperties());

        // Setup graph A and graph B with same schema
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true)
                        .build(), contextBlankUser());
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_B)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true)
                        .build(), contextBlankUser());

    }

    @Test
    public void shouldGetInfoForGraphId() throws Exception {
        // Given
        Map<String, Object> results = federatedStore.execute(
                new GetAllGraphInfo.Builder()
                        .graphIDsCSV(GRAPH_ID_A)
                        .build(), contextBlankUser());

        // When / Then
        assertThat(results)
                .containsOnlyKeys(List.of(GRAPH_ID_A));
    }

    @Test
    public void shouldNotGetInfoForBlackListGraphId() throws Exception {
        // Given
        Map<String, Object> results = federatedStore.execute(
                new GetAllGraphInfo.Builder()
                        .blackListGraphIdsCSV(GRAPH_ID_A)
                        .build(), contextBlankUser());

        // When / Then
        assertThat(results)
                .containsOnlyKeys(List.of(GRAPH_ID_B));
    }


    @Test
    public void shouldNotGetInfoForWhiteListWhileAlsoInBlackList() throws Exception {
        // Given
        Map<String, Object> results = federatedStore.execute(
                new GetAllGraphInfo.Builder()
                        .graphIDs(List.of(GRAPH_ID_A, GRAPH_ID_B))
                        .blackListGraphIdsCSV(GRAPH_ID_A)
                        .build(), contextBlankUser());

        // When / Then
        assertThat(results)
                .containsOnlyKeys(List.of(GRAPH_ID_B));
    }
}
