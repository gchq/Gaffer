/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetSchemaHandlerTest {
    private GetSchemaHandler handler;
    private Schema schema;
    private Store store;
    private Context context;
    private User user;
    private StoreProperties properties;
    private byte[] compactSchemaBytes;

    @Before
    public void setup() {
        handler = new GetSchemaHandler();
        store = mock(Store.class);
        context = mock(Context.class);
        user = mock(User.class);
        properties = new StoreProperties();
        schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .description("anEdge")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .description("anotherEdge")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(TestPropertyNames.PROP_1, "string")
                        .description("anEntity")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .aggregateFunction(new StringConcat())
                        .build())
                .type("true", Boolean.class)
                .build();
        compactSchemaBytes = schema.toCompactJson();
    }

    @Test
    public void shouldReturnCompactSchema() throws OperationException {
        given(store.getProperties()).willReturn(properties);
        given(store.getSchema()).willReturn(schema);
        given(context.getUser()).willReturn(user);

        final GetSchema operation = new GetSchema.Builder()
                .compact(true)
                .build();

        // When
        final Schema result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        JsonAssert.assertNotEqual(schema.toJson(true), result.toJson(true));
        JsonAssert.assertEquals(compactSchemaBytes, result.toJson(true));
    }

    @Test
    public void shouldReturnFullSchema() throws OperationException {
        given(store.getProperties()).willReturn(properties);
        given(store.getOriginalSchema()).willReturn(schema);
        given(context.getUser()).willReturn(user);

        final GetSchema operation = new GetSchema();

        // When
        final Schema result = handler.doOperation(operation, context, store);

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(schema.toJson(true), result.toJson(true));
    }

    @Test
    public void shouldThrowExceptionForNullOperation() throws OperationException {
        final GetSchema operation = null;

        // When / Then
        try {
            handler.doOperation(operation, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Operation cannot be null"));
        }
    }
}
