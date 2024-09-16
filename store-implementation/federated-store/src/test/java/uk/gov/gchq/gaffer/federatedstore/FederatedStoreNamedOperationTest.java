/*
 * Copyright 2023 Crown Copyright
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

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreNamedOperationTest {

    public static final String SELF_REFERENCING_NAMED_OP = "selfRefNamedOp";
    public static final String OUTER = "outer";
    public static final String INNER = "inner";

    @BeforeEach
    public void before() {
        resetForFederatedTests();
    }

    @Test
    public void shouldFailWhenAddingSelfReferencingNamedOperationWhenNestedNamedOpsIsAllowed() throws Exception {

        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setNestedNamedOperationAllow(true);

        final Graph federatedStore = new Graph.Builder()
                .config(new GraphConfig(GRAPH_ID_TEST_FEDERATED_STORE))
                .addSchema(new Schema())
                .storeProperties(properties)
                .build();

        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() ->
                        federatedStore.execute(new AddNamedOperation.Builder()
                                .name(SELF_REFERENCING_NAMED_OP)
                                .operationChain(new OperationChain.Builder()
                                        .first(new NamedOperation.Builder()
                                                .name(SELF_REFERENCING_NAMED_OP)
                                                .build())
                                        .build())
                                .build(), contextTestUser()))
                .withMessageContaining("Self referencing namedOperations would cause infinitive loop. operationName:selfRefNamedOp");

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> federatedStore.execute(new NamedOperation.Builder()
                        .name(SELF_REFERENCING_NAMED_OP)
                        .build(), contextTestUser()))
                .withMessageContaining("The named operation: selfRefNamedOp was not found.");
    }

    @Test
    public void shouldFailWhenAddingAnyNestedNamedOperationWhenNestedNamedOpsIsNotAllowed() throws Exception {

        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setNestedNamedOperationAllow(false);

        final Graph federatedStore = new Graph.Builder()
                .config(new GraphConfig(GRAPH_ID_TEST_FEDERATED_STORE))
                .addSchema(new Schema())
                .storeProperties(properties)
                .build();

        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> federatedStore.execute(new AddNamedOperation.Builder()
                        .name(SELF_REFERENCING_NAMED_OP)
                        .operationChain(new OperationChain.Builder()
                                .first(new NamedOperation.Builder()
                                        .name(SELF_REFERENCING_NAMED_OP)
                                        .build())
                                .build())
                        .build(), contextTestUser()))
                .withMessageContaining("NamedOperations can not be nested within NamedOperations");

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> federatedStore.execute(new NamedOperation.Builder()
                        .name(SELF_REFERENCING_NAMED_OP)
                        .build(), contextTestUser()))
                .withMessageContaining("The named operation: selfRefNamedOp was not found.");

    }

    @Test
    public void shouldFailWhenUsingInnerNestedNamedOpBeforeItsAddedWhenNestedNamedOpsIsAllowed() throws Exception {

        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setNestedNamedOperationAllow(true);

        final Graph federatedStore = new Graph.Builder()
                .config(new GraphConfig(GRAPH_ID_TEST_FEDERATED_STORE))
                .addSchema(new Schema())
                .storeProperties(properties)
                .build();

        //ref inner before adding it
        federatedStore.execute(new AddNamedOperation.Builder()
                .name(OUTER)
                .operationChain(new OperationChain.Builder()
                        .first(new NamedOperation.Builder()
                                .name(INNER)
                                .build())
                        .build())
                .build(), contextTestUser());

        //use inner before adding
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> federatedStore.execute(new NamedOperation.Builder()
                        .name(OUTER)
                        .build(), contextTestUser()))
                .withMessageContaining("The named operation: inner was not found");
    }

    @Test
    public void shouldAllowRefInnerNestedNamedOpBeforeAddedAndUsingInnerNestedNamedOpAfterItsAddedWhenNestedNamedOpsIsAllowed() throws Exception {

        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setNestedNamedOperationAllow(true);

        final Graph federatedStore = new Graph.Builder()
                .config(new GraphConfig(GRAPH_ID_TEST_FEDERATED_STORE))
                .addSchema(new Schema())
                .storeProperties(properties)
                .build();

        //ref inner before adding it
        federatedStore.execute(new AddNamedOperation.Builder()
                .name(OUTER)
                .operationChain(new OperationChain.Builder()
                        .first(new NamedOperation.Builder()
                                .name(INNER)
                                .build())
                        .build())
                .build(), contextTestUser());

        federatedStore.execute(new AddNamedOperation.Builder()
                .name(INNER)
                .operationChain(new OperationChain.Builder()
                        .first(new GetAllElements())
                        .build())
                .build(), contextTestUser());

        //use inner after adding
        federatedStore.execute(new NamedOperation.Builder()
                .name(OUTER)
                .build(), contextTestUser());
    }

    @Test
    public void shouldFailDueToNestedOpsNotAllowed() throws Exception {

        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.setNestedNamedOperationAllow(false);

        final Graph federatedStore = new Graph.Builder()
                .config(new GraphConfig(GRAPH_ID_TEST_FEDERATED_STORE))
                .addSchema(new Schema())
                .storeProperties(properties)
                .build();

        federatedStore.execute(new AddNamedOperation.Builder()
                .name(INNER)
                .operationChain(new OperationChain.Builder()
                        .first(new GetAllElements())
                        .build())
                .build(), contextTestUser());

        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> federatedStore.execute(new AddNamedOperation.Builder()
                        .name(OUTER)
                        .operationChain(new OperationChain.Builder()
                                .first(new NamedOperation.Builder()
                                        .name(INNER)
                                        .build())
                                .build())
                        .build(), contextTestUser()))
                .withMessageContaining("NamedOperations can not be nested within NamedOperations");

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> federatedStore.execute(new NamedOperation.Builder()
                        .name(OUTER)
                        .build(), contextTestUser()))
                .withMessageContaining("The named operation: outer was not found.");
    }
}
