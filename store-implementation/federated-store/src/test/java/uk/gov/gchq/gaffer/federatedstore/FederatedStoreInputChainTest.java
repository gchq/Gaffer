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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.GetVariable;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.koryphe.impl.function.ToList;
import uk.gov.gchq.koryphe.impl.function.ToLowerCase;
import uk.gov.gchq.koryphe.impl.function.ToUpperCase;
import uk.gov.gchq.koryphe.impl.predicate.IsA;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.basicEntitySchema;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreInputChainTest {
    public static final String VAR = "variable";
    public static final String INPUT_STRING = "AbCd";
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

        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true)
                        .build(), contextBlankUser());
    }

    @Test
    public void shouldCorrectlySetGenericInputInOperationChain() throws OperationException {
        // Given / When
        final Object output = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new SetVariable.Builder()
                                .input(INPUT_STRING)
                                .variableName(VAR)
                                .build())
                        .then(new GetVariable.Builder()
                                .variableName(VAR)
                                .build())
                        .then((Input<? super Object>) new If.Builder()
                                .conditional(new Conditional(new IsA(STRING)))
                                .then(new Map<>(Lists.newArrayList(new ToUpperCase(), new ToList())))
                                .otherwise(new Map<>(Lists.newArrayList(new ToLowerCase(), new ToList())))
                                .build())
                        .build(), contextBlankUser());

        // Then
        assertThat(output)
                .isInstanceOf(List.class)
                .isEqualTo(Lists.newArrayList(INPUT_STRING.toUpperCase()));
    }

    @Test
    public void shouldCorrectlySetGenericInputInOperationChainWithFederatedOperation() throws OperationException {
        // Given / When
        final Object output = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new SetVariable.Builder()
                                .input(INPUT_STRING)
                                .variableName(VAR)
                                .build())
                        .then(new GetVariable.Builder()
                                .variableName(VAR)
                                .build())
                        .then(new FederatedOperation.Builder()
                                .op(new If.Builder()
                                        .conditional(new Conditional(new IsA(STRING)))
                                        .then(new Map<>(Lists.newArrayList(new ToUpperCase(), new ToList())))
                                        .otherwise(new Map<>(Lists.newArrayList(new ToLowerCase(), new ToList())))
                                        .build())
                                .build())
                        .build(), contextBlankUser());

        // Then
        assertThat(output)
                .isInstanceOf(List.class)
                .isEqualTo(Lists.newArrayList(INPUT_STRING.toUpperCase()));
    }

    @Test
    public void shouldCorrectlySetMultiInputInOperationChain() throws OperationException {
        // Given / When
        final Iterable<? extends Element> output = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new SetVariable.Builder()
                                .input(Lists.newArrayList(entityBasic()))
                                .variableName(VAR)
                                .build())
                        .then(new GetVariable.Builder()
                                .variableName(VAR)
                                .build())
                        .thenTypeUnsafe(new AddElements())
                        .then(new DiscardOutput())
                        .then(new GetAllElements())
                        .build(), contextBlankUser());

        // Then
        assertThat((Iterable<Entity>) output).containsExactly(entityBasic());
    }

    @Test
    public void shouldCorrectlySetMultiInputInOperationChainWithFederatedOperation() throws OperationException {
        // Given / When
        final Iterable<? extends Element> output = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new SetVariable.Builder()
                                .input(Lists.newArrayList(entityBasic()))
                                .variableName(VAR)
                                .build())
                        .then(new GetVariable.Builder()
                                .variableName(VAR)
                                .build())
                        .then(new FederatedOperation.Builder()
                                .op(new AddElements())
                                .build())
                        .then(new DiscardOutput())
                        .then(new GetAllElements())
                        .build(), contextBlankUser());

        // Then
        assertThat((Iterable<Entity>) output).containsExactly(entityBasic());
    }
}
