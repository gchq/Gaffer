/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddSchemaToLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddSchemaToLibrary.Builder;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.StoreUser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddSchemaToLibraryHandlerTest {

    private static final String TEST_SCHEMA_ID = "testSchemaId";
    private static final String TEST_STORE_ID = "testStoreId";

    private Store store;
    private Schema schema;

    @BeforeEach
    public void setUp() throws Exception {
        HashMapGraphLibrary.clear();
        store = new TestAddToGraphLibraryImpl();
        schema = new Schema.Builder().id(TEST_SCHEMA_ID).build();
    }

    @Test
    public void shouldThrowWithNoGraphLibrary() throws StoreException {
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());

        final Exception exception = assertThrows(UnsupportedOperationException.class, () ->
                store.execute(new Builder().schema(schema).id(TEST_SCHEMA_ID).build(), new Context(StoreUser.blankUser())));

        final String expected = String.format("Operation class %s is not supported by the %s.", AddSchemaToLibrary.class.getName(), TestAddToGraphLibraryImpl.class.getSimpleName());
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldAddSchemaWithGraphLibrary() throws StoreException, OperationException {
        // Given
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());

        // When
        store.execute(new Builder().schema(schema).id(TEST_SCHEMA_ID).build(), new Context(StoreUser.blankUser()));

        // Then
        Schema actualSchema = library.getSchema(TEST_SCHEMA_ID);
        assertEquals(schema, actualSchema);
    }

    @Test
    public void shouldSupportAddToGraphLibrary() throws StoreException {
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);

        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());

        assertTrue(store.isSupported(AddSchemaToLibrary.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryI() throws StoreException {
        // Given - GraphLibrary has not been set

        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());

        assertFalse(store.isSupported(AddSchemaToLibrary.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryII() throws StoreException {
        store.setGraphLibrary(new NoGraphLibrary());

        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());

        assertFalse(store.isSupported(AddSchemaToLibrary.class));
    }
}
