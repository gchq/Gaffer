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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.INTEGER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasicDefinition;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreGraphLibraryTest {

    public static final String SCHEMA_1 = "schema1";
    private FederatedStore federatedStore;

    @AfterAll
    public static void after() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        HashMapGraphLibrary library = new HashMapGraphLibrary();
        final AccumuloProperties properties = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
        library.addProperties(PROPERTY_1, properties.clone());
        final Schema build = new Schema.Builder()
                .entity(GROUP_BASIC_ENTITY, entityBasicDefinition())
                .type(STRING, new TypeDefinition.Builder().clazz(String.class).aggregateFunction(new StringConcat()).build())
                .type(INTEGER, new TypeDefinition.Builder().clazz(Integer.class).aggregateFunction(new Sum()).build())
                .build();
        library.addSchema(SCHEMA_1, build);
        library.add(GRAPH_ID_B, build, properties.clone());

        federatedStore = new FederatedStore();
        federatedStore.setGraphLibrary(library);

        FederatedStoreProperties fedProps = new FederatedStoreProperties();
        fedProps.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, fedProps);
    }

    @Test
    public void shouldErrorIfPropertiesIsNotInLibrary() throws Exception {
        //when
        final OperationException operationException = assertThrows(OperationException.class, () ->
                federatedStore.execute(
                        new AddGraph.Builder()
                                .graphId(GRAPH_ID_ACCUMULO)
                                .parentPropertiesId(PROPERTY_1 + "nope")
                                .parentSchemaIds(Lists.newArrayList(SCHEMA_1))
                                .isPublic(true).build(), contextTestUser()));
        //then
        assertThat(operationException).getRootCause().message().contains("StoreProperties could not be found in the graphLibrary with id: property1nope");
    }

    @Test
    public void shouldErrorIfSchemaIsNotInLibrary() throws Exception {
        final OperationException operationException = assertThrows(OperationException.class, () ->
                federatedStore.execute(
                        new AddGraph.Builder()
                                .graphId(GRAPH_ID_ACCUMULO)
                                .parentPropertiesId(PROPERTY_1)
                                .parentSchemaIds(Lists.newArrayList(SCHEMA_1 + "nope"))
                                .isPublic(true).build(), contextTestUser()));
        assertThat(operationException).getRootCause().message().contains("Schema could not be found in the graphLibrary with id: [schema1nope]");
    }

    @Test
    public void shouldAddGraphWithSchemaAndPropertyFromLibrary() throws Exception {
        //given
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .parentPropertiesId(PROPERTY_1)
                        .parentSchemaIds(Lists.newArrayList(SCHEMA_1))
                        .isPublic(true).build(), contextTestUser());


        federatedStore.execute(
                new FederatedOperation.Builder()
                        .op(new AddElements.Builder()
                                .input(entityBasic())
                                .build()).build()
                        .graphIdsCSV(GRAPH_ID_A), contextTestUser());


        final Iterable<? extends Element> allElements = federatedStore.execute(new GetAllElements(), contextTestUser());

        assertThat(allElements)
                .size().isEqualTo(1);
    }

    @Test
    public void shouldAddGraphWithGraphFromLibrary() throws Exception {
        //given
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_B)
                        .isPublic(true).build(), contextTestUser());


        federatedStore.execute(
                new FederatedOperation.Builder()
                        .op(new AddElements.Builder()
                                .input(entityBasic())
                                .build()).build()
                        .graphIdsCSV(GRAPH_ID_B), contextTestUser());


        final Iterable<? extends Element> allElements = federatedStore.execute(new GetAllElements(), contextTestUser());

        assertThat(allElements)
                .size().isEqualTo(1);
    }

}
