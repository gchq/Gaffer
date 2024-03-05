/*
 * Copyright 2024 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.addGraphToAccumuloStore;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.basicEntitySchema;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreWhileHandlerTest {
    @BeforeEach
    public void before() {
        resetForFederatedTests();
    }

    @Test
    void shouldWhileLoopOperationWithDistinctOperations() throws Exception {
        //given
        final FederatedStore federatedStore = new FederatedStore();
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), properties);

        addGraphToAccumuloStore(federatedStore, GRAPH_ID_ACCUMULO, true, basicEntitySchema());

        //1st Add Elements is distinct.
        federatedStore.execute(new AddElements.Builder()
                .input(entityBasic())
                .build(), contextTestUser());

        //2nd Add Elements is distinct.
        final Operation operation = new While.Builder()
                .operation(new
                        AddElements.Builder()
                        .input(entityBasic())
                        .build())
                .conditional(o -> true)
                .maxRepeats(2)
                .build();

        //when
        federatedStore.execute(operation, contextTestUser());

        //then
        assertThat(federatedStore.execute(new GetAllElements(), contextTestUser()))
                .containsExactly(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        .property(PROPERTY_1, 3)
                        .build());
    }

    @Test
    void shouldNotWhileLoopOperationWithUnDistinctOperations() throws Exception {
        //given
        final FederatedStore federatedStore = new FederatedStore();
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), properties);

        addGraphToAccumuloStore(federatedStore, GRAPH_ID_ACCUMULO, true, basicEntitySchema());

        final AddElements addElements = new AddElements.Builder()
                .input(entityBasic())
                .build();

        //1st Add Elements.
        federatedStore.execute(addElements, contextTestUser());

        //2nd Add Elements is same as 1st
        //This is Java OO reuse issue.
        final Operation operation = new While.Builder()
                .operation(addElements)
                .conditional(o -> true)
                .maxRepeats(2)
                .build();

        //when
        federatedStore.execute(operation, contextTestUser());


        //then
        assertThat(federatedStore.execute(new GetAllElements(), contextTestUser()))
                .containsExactly(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        // This isn't 3 because of Java Object reuse issue,
                        // Federation detects seeing the operation more than once
                        .property(PROPERTY_1, 1)
                        .build());
    }

    @Test
    void shouldWhileLoopOperationWithRepeatingConditionalOperation() throws Exception {
        //given
        final FederatedStore federatedStore = new FederatedStore();
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), properties);

        addGraphToAccumuloStore(federatedStore, GRAPH_ID_ACCUMULO, true, basicEntitySchema());

        federatedStore.execute(new AddElements.Builder()
                .input(entityBasic())
                .build(), contextTestUser());

        final Operation operation = new While.Builder()
                .operation(new AddElements.Builder()
                        .input(entityBasic())
                        .build())
                //This GetAllElements is re-occurring amd should not trigger a looping error.
                .conditional(new Exists(), new GetAllElements())
                .maxRepeats(2)
                .build();

        //when
        federatedStore.execute(operation, contextTestUser());

        //then
        assertThat(federatedStore.execute(new GetAllElements(), contextTestUser()))
                .containsExactly(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        .property(PROPERTY_1, 3)
                        .build());
    }
}
