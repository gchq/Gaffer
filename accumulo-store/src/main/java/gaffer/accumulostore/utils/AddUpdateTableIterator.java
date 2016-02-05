/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.utils;

import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.AccumuloStore;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

import java.util.EnumSet;

public final class AddUpdateTableIterator {

    private AddUpdateTableIterator() {
        // private to prevent this class being instantiated. All methods are static and should be called directly.
    }

    /**
     * This method takes a store and uses the default Aggregator iterator name found in {@link Constants} as the iterator name to be removed.
     * The stores configured iterator settings factory will be used to create a new iterator in the removed ones place
     * @param store
     * @throws StoreException
     */
    public static void updateIterator(final AccumuloStore store) throws StoreException {
        updateIterator(store, Constants.AGGREGATOR_ITERATOR_NAME);
    }

    /**
     * This method takes a store and the name of an Aggregator iterator to be removed.
     * The store's configured {@link gaffer.accumulostore.key.IteratorSettingFactory} factory will be used to create the new Aggregator iterator in the removed one's place
     * @param store
     * @param iteratorName
     * @throws StoreException
     */
    public static void updateIterator(final AccumuloStore store, final String iteratorName) throws StoreException {
        try {
            updateIterator(store, iteratorName, store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
            //Update GafferUtilsTable with likely new schemas
            TableUtils.addUpdateUtilsTable(store);
        } catch (IteratorSettingException | TableUtilException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * This method takes a store and the name of an iterator to be removed.
     * The provided {@link org.apache.accumulo.core.client.IteratorSetting} will be used to create an iterator in the removed ones place.
     * @param store
     * @param iteratorName
     * @param iteratorSetting
     * @throws StoreException
     */
    public static void updateIterator(final AccumuloStore store, final String iteratorName, final IteratorSetting iteratorSetting) throws StoreException {
        try {
            store.getConnection().tableOperations().removeIterator(store.getProperties().getTable(), iteratorName, EnumSet.of(IteratorScope.majc, IteratorScope.minc, IteratorScope.scan));
        } catch (AccumuloSecurityException | AccumuloException
                | TableNotFoundException | StoreException e) {
            throw new StoreException("Unable remove iterator with Name: " + iteratorName);
        }
        addIteratorSetting(store, iteratorSetting);
    }

    /**
     * This should be used if a gaffer version upgrade causes the aggregator iterator to be removed from a table
     *
     * @param store
     * @throws StoreException
     */
    public static void addAggregatorIterator(final AccumuloStore store) throws StoreException {
        try {
            addIteratorSetting(store, store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
        } catch (IteratorSettingException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * This method can be used to attach an iterator to the table in use by the store instance.
     *
     * @param store
     * @param iteratorSetting
     * @throws StoreException
     */
    public static void addIteratorSetting(final AccumuloStore store, final IteratorSetting iteratorSetting) throws StoreException {
        try {
            store.getConnection().tableOperations().attachIterator(store.getProperties().getTable(), iteratorSetting);
        } catch (AccumuloSecurityException | AccumuloException| TableNotFoundException e) {
            throw new StoreException("Add iterator with Name: " + iteratorSetting.getName());
        }
    }
}
