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

package uk.gov.gchq.gaffer.sparkaccumulo;

import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

public abstract class AbstractPropertiesDrivenTest {

    private static Store store;

    public static StoreProperties setUpBeforeClass(String propertiesID) throws Exception {
        // Get the store class from the properties supplied
        Class currentClass = new Object() { }.getClass().getEnclosingClass();
        StoreProperties suppliedProperties = StoreProperties
                .loadStoreProperties(currentClass.getResourceAsStream(propertiesID));
        final String storeClass = suppliedProperties.getStoreClass();
        if (null == storeClass) {
            throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS);
        }
        // Instantiate the store class
        try {
            store = Class.forName(storeClass)
                    .asSubclass(Store.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
        }
        // Set up the data store and set the properties to suit.
        return (StoreProperties) store.setUpTestDB(suppliedProperties);
    }

    public static void tearDownAfterClass() throws Exception {
        store.tearDownTestDB();
    }

}
