/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FederatedGetSchemaHandlerTest {
    private FederatedGetSchemaHandler handler;
    private FederatedStore fStore;
    private Context context;
    private User user;
    private StoreProperties properties;
    private AccumuloProperties accProperties;

    private final String TEST_FED_STORE = "testFedStore";
    private final HashMapGraphLibrary library = new HashMapGraphLibrary();
    private final Schema STRING_SCHEMA = new Schema.Builder()
            .type("string", new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();

    @Before
    public void setup() throws StoreException {
        handler = new FederatedGetSchemaHandler();
        user = new User("testUser");
        context = new Context(user);
        properties = new FederatedStoreProperties();
        accProperties = new AccumuloProperties();

        accProperties.setId("accProp");
        accProperties.setStoreClass(MockAccumuloStore.class);
        accProperties.setStorePropertiesClass(AccumuloProperties.class);

        fStore = new FederatedStore();
        fStore.initialise(TEST_FED_STORE, null, properties);

        library.clear();
    }

    @Test
    public void shouldReturnSchema() throws OperationException {
        library.addProperties(accProperties);
        fStore.setGraphLibrary(library);

        final Schema edgeSchema = new Schema.Builder()
                .id("edgeSchema")
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .property("prop1", "string")
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema(edgeSchema);

        fStore.execute(Operation.asOperationChain(
                new AddGraph.Builder()
                .graphId("schema")
                .parentPropertiesId("accProp")
                .parentSchemaIds(Lists.newArrayList("edgeSchema"))
                .build()), context);

        final GetSchema operation = new GetSchema.Builder()
                .compact(true)
                .build();

        // When
        final Schema result = handler.doOperation(operation, context, fStore);

        // Then
        assertNotNull(result);
        assertArrayEquals(edgeSchema.toJson(true), result.toJson(true));
    }

    @Test
    public void shouldReturnAllSchemasForANullOperation() throws OperationException {
        library.addProperties(accProperties);
        fStore.setGraphLibrary(library);

        final GetSchema operation = null;

        try {
            handler.doOperation(operation, context, fStore);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Operation cannot be null"));
        }
    }
}
