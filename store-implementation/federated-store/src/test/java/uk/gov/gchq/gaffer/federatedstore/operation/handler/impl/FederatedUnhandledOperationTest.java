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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;

public class FederatedUnhandledOperationTest {
    private FederatedStoreProperties federatedStoreProperties;
    private static final AccumuloProperties PROPERTIES = FederatedStoreTestUtil.loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    @BeforeEach
    public void setUp() throws Exception {
        FederatedStoreTestUtil.resetForFederatedTests();
        federatedStoreProperties = getFederatedStorePropertiesWithHashMapCache();
    }

    @AfterEach
    void afterEach() {
        FederatedStoreTestUtil.resetForFederatedTests();
    }

    @Test
    public void shouldFailWithUnsupportedOperation() throws Exception {
        final String testExceptionMessage = "TestExceptionMessage";

        //make custom store with no GetAllElements
        FederatedStore federatedStoreWithNoGetAllElements = new FederatedStore() {
            @Override
            protected void addAdditionalOperationHandlers() {
                super.addAdditionalOperationHandlers();
                //No support for GetAllElements
                addOperationHandler(GetAllElements.class, null);
                //No support for adding Generic Handlers to Federation.
                addOperationHandler(AddGraph.class, new FederatedAddGraphHandler() {
                    @Override
                    protected void addGenericHandler(final FederatedStore store, final Graph graph) {
                        //nothing
                    }
                });
            }

            @Override
            protected Object doUnhandledOperation(final Operation operation, final Context context) {
                throw new IllegalStateException(testExceptionMessage);
            }
        };
        federatedStoreWithNoGetAllElements.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedStoreProperties);

        //Add graph.
        federatedStoreWithNoGetAllElements.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .schema(loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON))
                        .storeProperties(PROPERTIES)
                        .build(), contextTestUser());

        //Add elements
        federatedStoreWithNoGetAllElements.execute(new AddElements.Builder().input(entityBasic()).build(), contextTestUser());


        // check handler is missing
        assertThatException()
                .isThrownBy(() -> federatedStoreWithNoGetAllElements.execute(new GetAllElements(), contextTestUser()))
                .withMessageContaining(testExceptionMessage);
    }

    @Test
    public void shouldDelegateUnknownOperationToSubGraphs() throws Exception {
        //make custom store with no GetAllElements
        FederatedStore federatedStoreWithNoGetAllElements = new FederatedStore() {
            @Override
            protected void addAdditionalOperationHandlers() {
                super.addAdditionalOperationHandlers();
                //No support for GetAllElements
                addOperationHandler(GetAllElements.class, null);
                //No support for adding Generic Handlers to Federation.
                addOperationHandler(AddGraph.class, new FederatedAddGraphHandler() {
                    @Override
                    protected void addGenericHandler(final FederatedStore store, final Graph graph) {
                        //nothing
                    }
                });
            }
        };
        federatedStoreWithNoGetAllElements.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedStoreProperties);

        //Add graph
        federatedStoreWithNoGetAllElements.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .schema(loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON))
                        .storeProperties(PROPERTIES)
                        .build(), contextTestUser());

        //Add elements
        federatedStoreWithNoGetAllElements.execute(new AddElements.Builder().input(entityBasic()).build(), contextTestUser());

        //Get Elements with no handler.
        assertThat(federatedStoreWithNoGetAllElements.execute(new GetAllElements(), contextTestUser()))
                .containsExactly(entityBasic());
    }
}
