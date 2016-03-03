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

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;

/**
 * This class is designed to update iterator settings for iterators set on a
 * table.
 * <p>
 * This class also has an executable main method that can be used to either
 * re-add or update the aggregator iterator that is set on a table The main
 * method takes 4 arguments, a path to a data schema, a path to a store schema
 * and path to a store properties file. In addition the main method takes one
 * other argument one word either add or update
 * <p>
 * The add option will set a new aggregator iterator on the table given in the
 * store properties file (For example if the iterator was removed in the
 * accumulo shell) The update option will update the existing aggregator
 * iterator with options for the store and data schemas provided previously to
 * the main method.
 * <p>
 * This is useful if you wish to change the way data is aggregated after you
 * have put some data in a table.
 */
public final class AddUpdateTableIterator {
    private AddUpdateTableIterator() {
        // private to prevent this class being instantiated. All methods are
        // static and should be called directly.
    }

    /**
     * This method takes a store and uses the default Aggregator iterator name
     * found in {@link AccumuloStoreConstants} as the iterator name to be removed. The stores
     * configured iterator settings factory will be used to create a new
     * iterator in the removed ones place
     *
     * @param store the accumulo store
     * @throws StoreException if any issues occur when updating the iterator
     */
    public static void updateIterator(final AccumuloStore store) throws StoreException {
        updateIterator(store, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
    }

    /**
     * This method takes a store and the name of an Aggregator iterator to be
     * removed. The store's configured
     * {@link gaffer.accumulostore.key.IteratorSettingFactory} factory will be
     * used to create the new Aggregator iterator in the removed one's place
     *
     * @param store        the accumulo store
     * @param iteratorName the name of the iterator update
     * @throws StoreException if any issues occur when updating the iterator
     */
    public static void updateIterator(final AccumuloStore store, final String iteratorName) throws StoreException {
        try {
            updateIterator(store, iteratorName,
                    store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
            // Update GafferUtilsTable with likely new schemas
            TableUtils.addUpdateUtilsTable(store);
        } catch (IteratorSettingException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * This method takes a store and the name of an iterator to be removed. The
     * provided {@link org.apache.accumulo.core.client.IteratorSetting} will be
     * used to create an iterator in the removed ones place.
     *
     * @param store           the accumulo store
     * @param iteratorName    the name of the iterator update
     * @param iteratorSetting the iterator setting to add
     * @throws StoreException if any issues occur when removing the given iterator name
     */
    public static void updateIterator(final AccumuloStore store, final String iteratorName,
            final IteratorSetting iteratorSetting) throws StoreException {
        try {
            store.getConnection().tableOperations().removeIterator(store.getProperties().getTable(), iteratorName,
                    EnumSet.of(IteratorScope.majc, IteratorScope.minc, IteratorScope.scan));
        } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException | StoreException e) {
            throw new StoreException("Unable remove iterator with Name: " + iteratorName);
        }
        addIteratorSetting(store, iteratorSetting);
    }

    /**
     * This should be used if a gaffer version upgrade causes the aggregator
     * iterator to be removed from a table
     *
     * @param store the accumulo store
     * @throws StoreException if any issues occur adding an aggregator iterator
     */
    public static void addAggregatorIterator(final AccumuloStore store) throws StoreException {
        try {
            addIteratorSetting(store, store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
        } catch (final IteratorSettingException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * This method can be used to attach an iterator to the table in use by the
     * store instance.
     *
     * @param store           the accumulo store
     * @param iteratorSetting the iterator setting to add.
     * @throws StoreException if any issues occur adding an iterator setting
     */
    public static void addIteratorSetting(final AccumuloStore store, final IteratorSetting iteratorSetting)
            throws StoreException {
        try {
            store.getConnection().tableOperations().attachIterator(store.getProperties().getTable(), iteratorSetting);
        } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
            throw new StoreException("Add iterator with Name: " + iteratorSetting.getName(), e);
        }
    }

    public static void main(final String[] args) throws StoreException, SchemaException, IOException {
        if (args.length < 4) {
            System.err.println("Wrong number of arguments. \nUsage: "
                    + "<data_schema_path> <store_schema_path> <store_properties_path> <option add update>");
            System.exit(1);
        }

        final AccumuloStore store = new AccumuloStore();
        store.initialise(DataSchema.fromJson(getDataSchemaPath(args)), StoreSchema.fromJson(getStoreSchemaPath(args)),
                StoreProperties.loadStoreProperties(getAccumuloPropertiesPath(args)));
        if ("update".equals(args[3])) {
            updateIterator(store);
        } else if ("add".equals(args[3])) {
            addAggregatorIterator(store);
        } else {
            throw new IllegalArgumentException("Supplied option must either be add or update");
        }
    }

    private static Path getAccumuloPropertiesPath(final String[] args) {
        return Paths.get(args[2]);
    }

    private static Path getStoreSchemaPath(final String[] args) {
        return Paths.get(args[1]);
    }

    private static Path getDataSchemaPath(final String[] args) {
        return Paths.get(args[0]);
    }
}
