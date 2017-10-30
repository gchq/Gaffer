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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddSchema;
import uk.gov.gchq.gaffer.store.operation.add.AddSchema.Builder;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.StoreUser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddSchemaHandlerTest {

    public static final String TEST_SCHEMA_ID = "testSchemaId";
    public static final String TEST_STORE_ID = "testStoreId";
    private Store store;
    private Schema schema;

    @Before
    public void setUp() throws Exception {
        HashMapGraphLibrary.clear();
        store = new TestAddToGraphLibraryImpl();
        schema = new Schema.Builder().id(TEST_SCHEMA_ID).build();
    }

    @Test
    public void shouldThrowWithNoGraphLibrary() throws Exception {
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        try {
            store.execute(new Builder().schema(schema).build(), new Context(StoreUser.blankUser()));
            fail("Exception expected");
        } catch (final Exception e) {
            assertEquals(e.getMessage(), String.format("Operation class %s is not supported by the %s.", AddSchema.class.getName(), TestAddToGraphLibraryImpl.class.getSimpleName()));
        }
    }

    @Test
    public void shouldAddSchemaWithGraphLibrary() throws Exception {
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        store.execute(new Builder().schema(schema).build(), new Context(StoreUser.blankUser()));
        Schema actualSchema = library.getSchema(TEST_SCHEMA_ID);
        assertEquals(schema, actualSchema);
    }

    @Test
    public void shouldSupportAddToGraphLibrary() throws Exception {
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        assertTrue(store.isSupported(AddSchema.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryI() throws Exception {
        //GraphLibrary has not been set
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        assertFalse(store.isSupported(AddSchema.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryII() throws Exception {
        store.setGraphLibrary(new NoGraphLibrary());
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        assertFalse(store.isSupported(AddSchema.class));
    }
}