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


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.addGraph;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;

public class FederatedStoreViewAggregationTest {

    private FederatedStore federatedStore;
    private Entity entity1, entity99, entityOther;

    @BeforeEach
    public void before() throws Exception {
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

    @Disabled //TODO FD SUPER remove ignore
    @Test
    public void shouldOnlyReturn1EntitySmallerThanViewFilter() throws Exception {
        //given
        addGraph(federatedStore, GRAPH_ID_A, true, loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON));
        addGraph(federatedStore, GRAPH_ID_B, true, loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON));

        addEntity(GRAPH_ID_A, entity1);
        addEntity(GRAPH_ID_B, entity99);
        addEntity(GRAPH_ID_B, entityOther);

        //when
        final Iterable elementsWithPropertyLessThan2 = federatedStore.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(GROUP_BASIC_ENTITY,
                                new ViewElementDefinition.Builder()
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select(PROPERTY_1)
                                                .execute(new IsLessThan(2))
                                                .build())
                                        .build())
                        .build())
                .build(), contextTestUser());

        final Iterable elementsWithPropertyLessThan100 = federatedStore.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(GROUP_BASIC_ENTITY,
                                new ViewElementDefinition.Builder()
                                        .postAggregationFilter(new ElementFilter.Builder()
                                                .select(PROPERTY_1)
                                                .execute(new IsLessThan(100))
                                                .build())
                                        .build())
                        .build())
                .build(), contextTestUser());

        //then
        assertThat(elementsWithPropertyLessThan2)
                .isNotNull()
                .withFailMessage("should return entity with property 1 which is less than view filter 2")
                .contains(entity1)
                .withFailMessage("should not return entity with property 99 which is more than view filter 2")
                .doesNotContain(entity99)
                .withFailMessage("should contain only 1 ")
                .hasSize(1);

        assertThat(elementsWithPropertyLessThan100)
                .isNotNull()
                .withFailMessage("should return entity \"basicVertexOther\" with property 99, which is less than view filter 100")
                .contains(entityOther)
                .withFailMessage("should not return entity \"basicVertex\" with un-aggregated property 1 or 99")
                .doesNotContain(entity1, entity99)
                .withFailMessage("should not return entity \"basicVertex\" with an aggregated property 100, which is less than view filter 100")
                .doesNotContain(new Entity.Builder()
                        .group(GROUP_BASIC_ENTITY)
                        .vertex(BASIC_VERTEX)
                        .property(PROPERTY_1, 100)
                        .build())
                .hasSize(1);
    }

    private void addEntity(final String graphIdA, final Entity entity) throws OperationException {
        federatedStore.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(entity)
                        .build())
                .graphIds(graphIdA)
                .build(), contextTestUser());
    }

    @Disabled //TODO FS remove ignore
    @Test
    public void shouldBeAwareOfIssuesWithViewsThatTransformsData() throws Exception {
        fail("TBA");
    }
}
