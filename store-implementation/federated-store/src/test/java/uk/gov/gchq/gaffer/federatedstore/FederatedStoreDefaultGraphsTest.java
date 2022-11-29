/*
 * Copyright 2021-2022 Crown Copyright
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphInfo;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.*;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.util.Collections;

public class FederatedStoreDefaultGraphsTest {

    private static final String STORE_PROP_ID = "storeProp";

    private static final Schema STRING_SCHEMA = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();

    private static final StoreProperties PROPERTIES = loadStoreProperties("properties/singleUseMapStore.properties");

    private final HashMapGraphLibrary library = new HashMapGraphLibrary();

    private FederatedStore federatedStore;

    @BeforeEach
    public void before() throws Exception {
        federatedStore = loadFederatedStoreFrom("configuredGraphIds.json");
        federatedStore.initialise(FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE, new Schema(), new FederatedStoreProperties());
    }

    // should only see the defaultJsonGraphId
    // TODOs:
    // write tests to check that users can see graphs
    // write tests to check that users can see specific graph

    @Test
    public void shouldGetDefaultedGraphIdFromJsonConfig() throws Exception {
        //Given
        FederatedStore defaultedFederatedStore = loadFederatedStoreFrom("configuredGraphIds.json");
        defaultedFederatedStore.initialise("defaultedFederatedStore", new Schema(), new FederatedStoreProperties());
        assertThat(defaultedFederatedStore)
                .isNotNull()
                .returns(Lists.newArrayList("defaultJsonGraphId"), from(FederatedStore::getStoreConfiguredGraphIds));

        //when
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> defaultedFederatedStore.getGraphs(testUser(), null, new GetAllGraphInfo()));
        //then
        assertThat(exception).message().contains("The following graphIds are not visible or do not exist: [defaultJsonGraphId]");
    }

    @Test
    public void shouldReturnSchemaOnlyForChosenGraphs() throws OperationException {
        library.addProperties(STORE_PROP_ID, PROPERTIES);
        federatedStore.setGraphLibrary(library);

        final Schema edgeSchema1 = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .property(PROPERTY_1, STRING)
                        .directed(DIRECTED_EITHER)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema("edgeSchema1", edgeSchema1);

        final Schema edgeSchema2 = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .property(PROPERTY_2, STRING)
                        .directed(DIRECTED_EITHER)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema("edgeSchema2", edgeSchema2);

        federatedStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId("schemaEnabled")
                        .parentPropertiesId(STORE_PROP_ID)
                        .parentSchemaIds(singletonList("edgeSchema1"))
                        //.disabledByDefault(false)
                        .build()), contextTestUser());

        federatedStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId("schemaDisabled")
                        .parentPropertiesId(STORE_PROP_ID)
                        .parentSchemaIds(singletonList("edgeSchema2"))
                        //.disabledByDefault(true)
                        .build()), contextTestUser());

        final FederatedOperation<Object, Object> federatedOperation = new FederatedOperation.Builder()
                .op(new GetSchema.Builder()
                        .compact(true)
                        .build())
                .graphIds(Collections.singletonList("schemaEnabled"))
                .build();

        // When
        //final Schema result = (Schema) handler.doOperation(operation, contextTestUser(), federatedStore);
        final Schema result = (Schema) federatedStore.execute(federatedOperation, contextTestUser());

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(edgeSchema1.toJson(true), result.toJson(true));
    }
}
