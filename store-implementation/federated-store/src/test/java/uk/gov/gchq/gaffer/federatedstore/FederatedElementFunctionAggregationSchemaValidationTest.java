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


import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedElementFunctionWithGivenStore;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_C;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.addGraphToAccumuloStore;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedElementFunctionAggregationSchemaValidationTest {

    private FederatedStore federatedStore;
    private Entity entity1, entity99, entityOther;

    @BeforeEach
    public void before() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), new FederatedStoreProperties());

        entity1 = new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(BASIC_VERTEX)
                .property(PROPERTY_1, 1)
                .build();

        entity99 = new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(BASIC_VERTEX)
                .property(PROPERTY_1, 99)
                .build();

        entityOther = new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex("basicVertexOther")
                .property(PROPERTY_1, 99)
                .build();
    }

    @Test
    public void shouldOnlyReturn1EntitySmallerThanSchemaValidationLimit() throws Exception {

        //given
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_A, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_B, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));

        addEntity(GRAPH_ID_A, entity1);
        addEntity(GRAPH_ID_B, entity99);
        addEntity(GRAPH_ID_B, entityOther);

        //when
        final Iterable elementsWithPropertyLessThan100 = federatedStore.execute(new GetAllElements(), contextTestUser());

        //then
        assertThat(elementsWithPropertyLessThan100)
                .isNotNull()
                .withFailMessage("should not return entity \"basicVertex\" with un-aggregated property 1 or 99")
                .doesNotContain(entity1, entity99)
                .withFailMessage("should not return entity \"basicVertex\" with an aggregated property 100, which is less than view filter 100")
                .doesNotContain(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        .property(PROPERTY_1, 100)
                        .build())
                .withFailMessage("should return entity \"basicVertexOther\" with property 99, which is less than view filter 100")
                .containsExactly(entityOther);
    }

    @Test
    public void shouldNotReturnAnyElementsAfterInValidationInTemporaryMap() throws Exception {

        //given
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_A, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_B, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_C, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));

        addEntity(GRAPH_ID_A, entity99); // 99 is valid
        addEntity(GRAPH_ID_B, entity1); // 100 is not valid.
        addEntity(GRAPH_ID_C, entity1); // correct behavior 100 & 1 is invalid. returning 1 would be incorrect if 100 had been deleted.
        addEntity(GRAPH_ID_B, entityOther);

        //when
        final Iterable elementsWithPropertyLessThan100 = federatedStore.execute(new GetAllElements(), contextTestUser());

        //then
        assertThat(elementsWithPropertyLessThan100)
                .isNotNull()
                .withFailMessage("should not return entity \"basicVertex\" with un-aggregated property 1 or 99")
                .doesNotContain(entity1, entity99)
                .withFailMessage("should not return entity \"basicVertex\" with an aggregated property 100, which is less than view filter 100")
                .doesNotContain(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        .property(PROPERTY_1, 100)
                        .build())
                .withFailMessage("should return entity \"basicVertexOther\" with property 99, which is less than view filter 100")
                .containsExactly(entityOther)
                .hasSize(1);
    }

    @Test
    public void shouldNotCurrentlySupportOtherTempResultsGraph() throws Exception {

        final byte[] givenResultsGraph = new JavaSerialiser().serialise(new GraphSerialisable.Builder()
                .config(new GraphConfig("TheGivenResultsGraph"))
                .schema(loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"))
                .properties(FederatedStoreTestUtil.loadAccumuloStoreProperties(FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                .build());

        //given
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_A, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_B, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));
        addGraphToAccumuloStore(federatedStore, GRAPH_ID_C, true, loadSchemaFromJson("/schema/basicEntityValidateLess100Schema.json"));

        addEntity(GRAPH_ID_A, entity99); // 99 is valid
        addEntity(GRAPH_ID_B, entity1); // 100 is not valid.
        addEntity(GRAPH_ID_C, entity1); // correct behavior 100 & 1 is invalid. returning 1 would be incorrect if 100 had been deleted.
        addEntity(GRAPH_ID_B, entityOther);

        //when
        Assertions.assertThatException()
                .isThrownBy(() -> federatedStore.execute(new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .option(FederatedStoreUtil.GIVEN_MERGE_STORE, givenResultsGraph.toString())
                        .mergeFunction(new FederatedElementFunctionWithGivenStore())
                        .build(), contextTestUser()))
                .withMessageContaining("Implementation of adding a different type of temporary merge graph is not yet implemented");
    }

    private void addEntity(final String graphIdA, final Entity entity) throws OperationException {
        federatedStore.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(entity)
                        .build())
                .graphIdsCSV(graphIdA)
                .build(), contextTestUser());
    }

}
