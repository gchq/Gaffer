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

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
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
import static uk.gov.gchq.gaffer.user.User.UNKNOWN_USER_ID;

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
    void shouldNotWhileLoopOperationWithUnDistinctJavaObjectOperations() throws Exception {
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
        //This is Java OO reuse.
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
                        .property(PROPERTY_1, 3)
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

    @Test
    void shouldWhileLoopOperationFromNamedOperation() throws Exception {
        //given
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());

        //Use graph due to hooks needed.
        final Graph federated = new Graph.Builder()
                .addSchema(new Schema())
                .config(new GraphConfig(GRAPH_ID_TEST_FEDERATED_STORE))
                .storeProperties(properties)
                .build();

        final Schema schema = basicEntitySchema();
        final StoreProperties storeProperties = FederatedStoreTestUtil.loadAccumuloStoreProperties(FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
        final Context context = new Context(new User(UNKNOWN_USER_ID));
        federated.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .storeProperties(storeProperties)
                        .schema(schema)
                        .parentSchemaIds(null)
                        .parentPropertiesId(null)
                        .isPublic(true)
                        .build()), context);

        federated.execute(new AddElements.Builder()
                .input(entityBasic())
                .build(), contextTestUser());

        //when
        final String savedWhileOp = "savedWhileOp";
        federated.execute(
                new AddNamedOperation.Builder()
                        .name(savedWhileOp)
                        .operationChain(
                                OperationChain.wrap(
                                        new While.Builder()
                                                .operation(new AddElements.Builder()
                                                        .input(entityBasic())
                                                        .build())
                                                //This GetAllElements is re-occurring amd should not trigger a looping error.
                                                .conditional(new Exists(), new GetAllElements())
                                                .maxRepeats(2)
                                                .build()))
                        .build(), contextTestUser());

        federated.execute(new NamedOperation.Builder().name(savedWhileOp).build(), contextTestUser());

        //then
        assertThat(federated.execute(new GetAllElements(), contextTestUser()))
                .containsExactly(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        .property(PROPERTY_1, 3)
                        .build());
    }
}
