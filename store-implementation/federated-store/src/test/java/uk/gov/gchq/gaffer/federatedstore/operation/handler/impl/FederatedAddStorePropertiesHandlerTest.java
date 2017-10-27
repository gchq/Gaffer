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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddStoreProperties;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreUser.blankUser;
import static uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddStorePropertiesHandler.ERROR_ADDING_STORE_TO_FEDERATED_STORE_S;
import static uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddStorePropertiesHandler.THE_STORE_DOES_NOT_HAVE_A_GRAPH_LIBRARY;

public class FederatedAddStorePropertiesHandlerTest {

    public static final String TEST_PROPS_ID = "testPropsId";
    private FederatedStore federatedStore;
    private StoreProperties props;

    @Before
    public void setUp() throws Exception {
        federatedStore = new FederatedStore();
        HashMapGraphLibrary.clear();
        props = new MapStoreProperties();
        props.setId(TEST_PROPS_ID);
        props.set("unusual", "testValue");
    }

    @Test
    public void shouldThrowWithNoGraphLibrary() throws Exception {
        federatedStore.initialise("fedStoreId", null, new FederatedStoreProperties());
        try {
            federatedStore.execute(new AddStoreProperties.Builder().storeProperties(props).build(), new Context(blankUser()));
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains(String.format(ERROR_ADDING_STORE_TO_FEDERATED_STORE_S, THE_STORE_DOES_NOT_HAVE_A_GRAPH_LIBRARY)));
        }
    }

    @Test
    public void shouldAddSchemaWithGraphLibrary() throws Exception {
        HashMapGraphLibrary library = new HashMapGraphLibrary();
        federatedStore.setGraphLibrary(library);
        federatedStore.initialise("fedStoreId", null, new FederatedStoreProperties());
        federatedStore.execute(new AddStoreProperties.Builder().storeProperties(props).build(), new Context(blankUser()));
        StoreProperties actualProps = library.getProperties(TEST_PROPS_ID);
        assertEquals(props.getProperties(), actualProps.getProperties());
    }

}