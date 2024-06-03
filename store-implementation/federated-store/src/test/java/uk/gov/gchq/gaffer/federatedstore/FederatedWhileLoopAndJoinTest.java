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
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.function.Function;

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

public class FederatedWhileLoopAndJoinTest {
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
    void shouldWhileLoopOperationWithJoinOperation() throws Exception {
        //given
        final FederatedStoreProperties properties = FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache();

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

        final While<Object, Object> simplifiedWhileJoinTestOp = new While.Builder<>()
                .conditional(new Exists(), new GetAllElements())
                .operation(new Join.Builder<>()
                        .operation(new OperationChain.Builder()
                                .first(new DiscardOutput())
                                //Add 1 element
                                .then(new AddElements.Builder()
                                        .input(entityBasic())
                                        .build())
                                .build())
                        .matchKey(MatchKey.LEFT)
                        .flatten(true)
                        .matchMethod(new KeyFunctionMatch.Builder()
                                .firstKeyFunction(Function.identity())
                                .secondKeyFunction(Function.identity())
                                .build())
                        .joinType(JoinType.OUTER)
                        .build())
                //loop 4 times
                .maxRepeats(4)
                .build();
        //when
        federated.execute(simplifiedWhileJoinTestOp, contextTestUser());

        //then
        assertThat(federated.execute(new GetAllElements(), contextTestUser()))
                .containsExactly(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        // 1 * 4 = 4
                        .property(PROPERTY_1, 4)
                        .build());
    }

    @Test
    void shouldWhileLoopOperationFromNamedOperation() throws Exception {
        //given
        final FederatedStoreProperties properties = FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache();

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
