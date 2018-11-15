/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.store.operation.add.AddSchemaToLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddSchemaToLibrary.Builder;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.StoreUser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddSchemaToLibraryHandlerTest {

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
        //given
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        try {
            //when
            store.execute(new Builder().schema(schema).id(TEST_SCHEMA_ID).build(), new Context(StoreUser.blankUser()));
            fail("Exception expected");
        } catch (final Exception e) {
            //then
            assertEquals(String.format("Operation class %s is not supported by the %s.", AddSchemaToLibrary.class.getName(), TestAddToGraphLibraryImpl.class.getSimpleName()), e.getMessage());
        }
    }

    @Test
    public void shouldAddSchemaWithGraphLibrary() throws Exception {
        //given
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());

        //when
        store.execute(new Builder().schema(schema).id(TEST_SCHEMA_ID).build(), new Context(StoreUser.blankUser()));
        //then
        Schema actualSchema = library.getSchema(TEST_SCHEMA_ID);
        assertEquals(schema, actualSchema);
    }

    @Test
    public void shouldSupportAddToGraphLibrary() throws Exception {
        //given
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        //when
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        //then
        assertTrue(store.isSupported(AddSchemaToLibrary.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryI() throws Exception {
        //given
        //GraphLibrary has not been set
        //when
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        //then
        assertFalse(store.isSupported(AddSchemaToLibrary.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryII() throws Exception {
        //given
        store.setGraphLibrary(new NoGraphLibrary());
        //when
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        //then
        assertFalse(store.isSupported(AddSchemaToLibrary.class));
    }
}
