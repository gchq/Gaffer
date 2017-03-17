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

package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;

/**
 * This class is designed to update iterator settings for iterators set on a
 * table.
 * <p>
 * This class also has an executable main method that can be used to either
 * re-add or update the aggregator iterator that is set on a table.
 * It should be run on an accumulo cluster. The main
 * method takes 3 arguments: a comma separated list of paths to schemas,
 * a path to a store properties file and the type of operation to perform on the
 * table iterators - add, update or remove.
 * <p>
 * The add option will set new iterators on the table given in the
 * store properties file (For example if the iterator was removed in the
 * accumulo shell) The update option will update the existing aggregator
 * iterator with options for the store and data schemas provided previously to
 * the main method. The remove option allows an iterator to be removed.
 * <p>
 * This is useful if you wish to change the way data is aggregated or validated
 * after you have put some data in a table.
 */
public final class AddUpdateTableIterator {
    public static final String UPDATE_KEY = "update";
    public static final String REMOVE_KEY = "remove";
    public static final String ADD_KEY = "add";
    private static final int NUM_REQUIRED_ARGS = 3;
    private static final String[] ITERATORS = {
            AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME,
            AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME
    };

    private AddUpdateTableIterator() {
        // private to prevent this class being instantiated. All methods are
        // static and should be called directly.
    }

    /**
     * This method takes a store and the name of an iterator to be
     * updated. The store's configured
     * {@link uk.gov.gchq.gaffer.accumulostore.key.IteratorSettingFactory} factory will be
     * used to create the new iterator in the removed one's place
     *
     * @param store        the accumulo store
     * @param iteratorName the name of the iterator update
     * @throws StoreException if any issues occur when updating the iterator
     */
    public static void updateIterator(final AccumuloStore store, final String iteratorName) throws StoreException {
        try {
            updateIterator(store, iteratorName,
                    store.getKeyPackage().getIteratorFactory().getIteratorSetting(store, iteratorName));
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
        removeIterator(store, iteratorName);
        addIterator(store, iteratorSetting);
    }

    /**
     * This method takes a store and the name of an iterator to be
     * removed.
     *
     * @param store        the accumulo store
     * @param iteratorName the name of the iterator update
     * @throws StoreException if any issues occur when updating the iterator
     */
    public static void removeIterator(final AccumuloStore store, final String iteratorName) throws StoreException {
        try {
            if (store.getConnection().tableOperations().listIterators(store.getProperties().getTable()).containsKey(iteratorName)) {
                store.getConnection()
                        .tableOperations()
                        .removeIterator(store.getProperties()
                                        .getTable(), iteratorName,
                                EnumSet.of(IteratorScope.majc, IteratorScope.minc, IteratorScope.scan));
            }
        } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException | StoreException e) {
            throw new StoreException("Unable remove iterator with Name: " + iteratorName, e);
        }
    }

    /**
     * This should be used if a gaffer version upgrade causes the aggregator
     * iterator to be removed from a table
     *
     * @param store        the accumulo store
     * @param iteratorName the iterator name
     * @throws StoreException if any issues occur adding an aggregator iterator
     */
    public static void addIterator(final AccumuloStore store, final String iteratorName) throws StoreException {
        if (!AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME.equals(iteratorName) || store.getProperties().getEnableValidatorIterator()) {
            if (store.getSchema().hasAggregators()) {
                try {
                    addIterator(store, store.getKeyPackage().getIteratorFactory().getIteratorSetting(store, iteratorName));
                } catch (final IteratorSettingException e) {
                    throw new StoreException(e.getMessage(), e);
                }
            }
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
    public static void addIterator(final AccumuloStore store, final IteratorSetting iteratorSetting)
            throws StoreException {
        try {
            store.getConnection().tableOperations().attachIterator(store.getProperties().getTable(), iteratorSetting);
        } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
            throw new StoreException("Add iterator with Name: " + iteratorSetting.getName(), e);
        }
        TableUtils.setLocalityGroups(store);
    }

    public static void main(final String[] args) throws StoreException, SchemaException, IOException {
        if (args.length < NUM_REQUIRED_ARGS) {
            System.err.println("Wrong number of arguments. \nUsage: "
                    + "<comma separated schema paths> <store properties path> <"
                    + ADD_KEY + "," + REMOVE_KEY + " or " + UPDATE_KEY
                    + ">");
            System.exit(1);
        }

        final AccumuloStore store = new AccumuloStore();
        store.initialise(Schema.fromJson(getSchemaPaths(args)),
                AccumuloProperties.loadStoreProperties(getAccumuloPropertiesPath(args)));

        final String modifyKey = getModifyKey(args);
        switch (modifyKey) {
            case UPDATE_KEY:
                for (final String iterator : ITERATORS) {
                    updateIterator(store, iterator);
                }
                break;
            case ADD_KEY:
                for (final String iterator : ITERATORS) {
                    addIterator(store, iterator);
                }
                break;
            case REMOVE_KEY:
                for (final String iterator : ITERATORS) {
                    removeIterator(store, iterator);
                }
                break;
            default:
                throw new IllegalArgumentException("Supplied add or update key ("
                        + modifyKey + ") was not valid, it must either be "
                        + ADD_KEY + "," + REMOVE_KEY + " or " + UPDATE_KEY + ".");
        }
    }

    private static String getModifyKey(final String[] arg) {
        return arg[2];
    }

    private static Path getAccumuloPropertiesPath(final String[] args) {
        return Paths.get(args[1]);
    }

    private static Path[] getSchemaPaths(final String[] args) {
        final String[] pathStrs = args[0].split(",");
        final Path[] paths = new Path[pathStrs.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Paths.get(pathStrs[i]);
        }

        return paths;
    }
}
