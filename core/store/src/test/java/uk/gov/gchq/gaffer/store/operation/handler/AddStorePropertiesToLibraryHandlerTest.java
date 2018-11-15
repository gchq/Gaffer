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
import uk.gov.gchq.gaffer.store.operation.add.AddStorePropertiesToLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddStorePropertiesToLibrary.Builder;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.StoreUser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddStorePropertiesToLibraryHandlerTest {

    public static final String TEST_PROPS_ID = "testPropsId";
    public static final String TEST_STORE_ID = "testStoreId";
    private Store store;
    private StoreProperties props;

    @Before
    public void setUp() throws Exception {
        store = new TestAddToGraphLibraryImpl();
        HashMapGraphLibrary.clear();
        props = new StoreProperties();
        props.setId(TEST_PROPS_ID);
        props.set("unusual", "testValue");
    }

    @Test
    public void shouldThrowWithNoGraphLibrary() throws Exception {
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        try {
            store.execute(new Builder().storeProperties(props).id(TEST_PROPS_ID).build(), new Context(StoreUser.blankUser()));
            fail("Exception expected");
        } catch (final Exception e) {
            assertEquals(String.format("Operation class %s is not supported by the %s.", AddStorePropertiesToLibrary.class.getName(), TestAddToGraphLibraryImpl.class.getSimpleName()), e.getMessage());
        }
    }

    @Test
    public void shouldAddSchemaWithGraphLibrary() throws Exception {
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        store.execute(new Builder().storeProperties(props).id(TEST_PROPS_ID).build(), new Context(StoreUser.blankUser()));
        StoreProperties actualProps = library.getProperties(TEST_PROPS_ID);
        assertEquals(props.getProperties(), actualProps.getProperties());
    }

    @Test
    public void shouldSupportAddToGraphLibrary() throws Exception {
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        store.setGraphLibrary(library);
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        assertTrue(store.isSupported(AddSchemaToLibrary.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryI() throws Exception {
        //GraphLibrary has not been set
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        assertFalse(store.isSupported(AddSchemaToLibrary.class));
    }

    @Test
    public void shouldNotSupportAddToGraphLibraryII() throws Exception {
        store.setGraphLibrary(new NoGraphLibrary());
        store.initialise(TEST_STORE_ID, new Schema(), new StoreProperties());
        assertFalse(store.isSupported(AddSchemaToLibrary.class));
    }

}
