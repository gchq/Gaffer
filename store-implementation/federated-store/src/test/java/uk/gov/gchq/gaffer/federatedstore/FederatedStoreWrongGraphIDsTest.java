/*
 * Copyright 2017-2022 Crown Copyright
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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage.GRAPH_IDS_NOT_VISIBLE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

public class FederatedStoreWrongGraphIDsTest {

    public static final Entity EXPECTED_ENTITY = entityBasic();
    public static final String THERE_SHOULD_BE_ONE_ELEMENT = "There should be one expected element";
    public static final String INTEGER = "Integer";
    public static final String WRONG_GRAPH_ID = "x";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private FederatedStore federatedStore;

    @AfterAll
    public static void after() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();

        FederatedStoreProperties fedProps = new FederatedStoreProperties();
        fedProps.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, fedProps);
    }

    @Test
    public void shouldThrowWhenWrongGraphIDOptionIsUsed() throws Exception {
        //given
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(new Schema.Builder()
                                .entity(GROUP_BASIC_ENTITY, new SchemaEntityDefinition.Builder()
                                        .vertex(STRING)
                                        .property(PROPERTY_1, INTEGER)
                                        .build())
                                .type(STRING, String.class)
                                .type(INTEGER, new TypeDefinition.Builder().clazz(Integer.class).aggregateFunction(new Sum()).build())
                                .build())
                        .isPublic(true).build(), contextBlankUser());

        federatedStore.execute(
                new FederatedOperation.Builder()
                        .op(new AddElements.Builder()
                                .input(EXPECTED_ENTITY)
                                .build())
                        .graphIds(GRAPH_ID_ACCUMULO)
                        .build(), contextBlankUser());

        //when
        final Iterable<? extends Element> getAllElements = federatedStore.execute(new GetAllElements.Builder().build(), contextBlankUser());
        final Iterable<? extends Element> getAllElementsFromAccumuloGraph = federatedStore.execute(getFederatedOperation(new GetAllElements()).graphIdsCSV(GRAPH_ID_ACCUMULO), contextBlankUser());
        final Exception gettingElementsFromWrongGraph = assertThrows(IllegalArgumentException.class, () -> federatedStore.execute(getFederatedOperation(new GetAllElements()).graphIdsCSV(WRONG_GRAPH_ID), contextBlankUser()));
        final Exception addingElementsToWrongGraph = assertThrows(IllegalArgumentException.class, () -> federatedStore.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(EXPECTED_ENTITY)
                        .build())
                .graphIds(WRONG_GRAPH_ID)
                .build(), contextBlankUser()));

        //then
        assertThat(getAllElements)
                .withFailMessage(THERE_SHOULD_BE_ONE_ELEMENT)
                .size().isEqualTo(1)
                .returnToIterable()
                .first().isEqualTo(EXPECTED_ENTITY);

        assertThat(getAllElementsFromAccumuloGraph)
                .withFailMessage(THERE_SHOULD_BE_ONE_ELEMENT)
                .size().isEqualTo(1)
                .returnToIterable()
                .first().isEqualTo(EXPECTED_ENTITY);

        assertThat(gettingElementsFromWrongGraph).message().isEqualTo(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(WRONG_GRAPH_ID)));

        assertThat(addingElementsToWrongGraph).message().isEqualTo(String.format(GRAPH_IDS_NOT_VISIBLE, Sets.newHashSet(WRONG_GRAPH_ID)));
    }
}
