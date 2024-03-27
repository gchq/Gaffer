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
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreVisibilityTest;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;

public class FederatedUnhandledOperationTest {
    private Graph federatedGraph;

    private static final AccumuloProperties PROPERTIES = FederatedStoreTestUtil.loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    public static FederatedStoreProperties createProperties() {
        FederatedStoreProperties fedProps  = new FederatedStoreProperties();
        try {
            Properties props = new Properties();
            props.load(FederatedStoreVisibilityTest.class.getResourceAsStream("/properties/federatedStore.properties"));
            fedProps.setProperties(props);
        } catch (IOException e) {
            e.printStackTrace();
        }
        fedProps .setDefaultCacheServiceClass(CACHE_SERVICE_CLASS_STRING);
        return fedProps;
    }

    @BeforeEach
    public void setUp() throws Exception {
        FederatedStoreTestUtil.resetForFederatedTests();
        FederatedStoreProperties props = createProperties();

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(props)
                .build();
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
        FederatedStoreProperties props = createProperties();
        federatedStoreWithNoGetAllElements.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);

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
        FederatedStoreProperties props = createProperties();
        federatedStoreWithNoGetAllElements.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, props);

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
