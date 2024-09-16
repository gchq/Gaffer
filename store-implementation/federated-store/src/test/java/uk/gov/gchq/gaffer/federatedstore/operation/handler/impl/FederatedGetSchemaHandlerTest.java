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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class FederatedGetSchemaHandlerTest {
    private static final String ACC_PROP_ID = "accProp";
    private static final String EDGE_SCHEMA_ID = "edgeSchema";
    private static final String TEST_FED_STORE = "testFedStore";
    private static final Schema STRING_SCHEMA = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();
    private static final AccumuloProperties PROPERTIES = loadAccumuloStoreProperties("properties/accumuloStore.properties");
    private final HashMapGraphLibrary library = new HashMapGraphLibrary();
    private FederatedOutputHandler handler;
    private FederatedStore federatedStore;

    @BeforeEach
    public void setup() throws StoreException {
        resetForFederatedTests();

        handler = new FederatedOutputHandler<>(new Schema());
        StoreProperties properties = new FederatedStoreProperties();
        //properties.set(storeConfiguredId) needs to be added so it reads from the  configuredGraphIds JSON file
        properties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));

        federatedStore = new FederatedStore();
        federatedStore.initialise(TEST_FED_STORE, null, properties);

    }

    @AfterEach
    public void after() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnSchema() throws OperationException {
        library.addProperties(ACC_PROP_ID, PROPERTIES);
        federatedStore.setGraphLibrary(library);

        final Schema edgeSchema = new Schema.Builder()
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .source(STRING)
                        .destination(STRING)
                        .directed(DIRECTED_EITHER)
                        .property(PROPERTY_1, STRING)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema(EDGE_SCHEMA_ID, edgeSchema);

        federatedStore.execute(OperationChain.wrap(
                new AddGraph.Builder()
                        .graphId("schema")
                        .parentPropertiesId(ACC_PROP_ID)
                        .parentSchemaIds(singletonList(EDGE_SCHEMA_ID))
                        .build()), contextTestUser());

        final GetSchema operation = new GetSchema.Builder()
                .compact(true)
                .build();

        // When
        final Schema result = (Schema) handler.doOperation(operation, contextTestUser(), federatedStore);

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(edgeSchema.toJson(true), result.toJson(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowExceptionForANullOperation() throws OperationException {
        library.addProperties(ACC_PROP_ID, PROPERTIES);
        federatedStore.setGraphLibrary(library);

        final GetSchema operation = null;

        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(operation, contextTestUser(), federatedStore))
                .withStackTraceContaining("Operation cannot be null");

    }
}
