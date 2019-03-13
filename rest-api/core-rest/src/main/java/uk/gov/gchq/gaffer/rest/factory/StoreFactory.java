/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.util.Config;

/**
 * A {@code StoreFactory} creates instances of
 * {@link uk.gov.gchq.gaffer.store.Store} to be reused for all queries.
 */
public interface StoreFactory {

    static StoreFactory createStoreFactory() {
        final String storeFactoryClass =
                System.getProperty(SystemProperty.STORE_FACTORY_CLASS,
                SystemProperty.Store_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(storeFactoryClass)
                    .asSubclass(StoreFactory.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create graph " +
                    "factory from class: " + storeFactoryClass, e);
        }
    }

    /**
     * Create a new {@link Store} instance.
     *
     * @return the graph
     */
    default Store createStore() {
        return Store.createStore(new Config());
    }

    /**
     * Get the {@link Store} instance.
     *
     * @return the store
     */
    Store getStore();
}
