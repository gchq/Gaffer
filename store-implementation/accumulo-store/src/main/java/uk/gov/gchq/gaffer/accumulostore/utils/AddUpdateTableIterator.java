/*
 * Copyright 2016-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;

/**
 * This class is designed to update iterator settings for iterators set on a
 * table.
 * <p>
 * This class also has an executable main method that can be used to either
 * re-add or update the aggregator iterators that is set on a table.
 * It should be run on an accumulo cluster. The main
 * method takes 4 arguments: a graphId, a comma separated list of paths to schemas,
 * a path to a store properties file and the type of operation to perform on the
 * table iterators - add, update or remove.
 * <p>
 * The add option will set new iterators on the table given in the
 * store properties file (For example if the iterator was removed in the
 * accumulo shell) The update option will update the existing aggregator
 * iterator with options for the store and data schemas provided previously to
 * the main method. The remove option allows an iterator to be removed.
 * <p>
 * This is useful if you wish to change your schema or upgrade to a newer version
 * of Gaffer. See the Accumulo Store README for more information on what changes
 * to your schema you are allowed to make.
 */
public final class AddUpdateTableIterator {
    public static final String UPDATE_KEY = "update";
    public static final String REMOVE_KEY = "remove";
    public static final String ADD_KEY = "add";
    private static final int NUM_REQUIRED_ARGS = 4;
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
        } catch (final IteratorSettingException e) {
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
            if (store.getConnection().tableOperations().listIterators(store.getTableName()).containsKey(iteratorName)) {
                store.getConnection()
                        .tableOperations()
                        .removeIterator(store.getTableName(), iteratorName,
                                EnumSet.of(IteratorScope.majc, IteratorScope.minc, IteratorScope.scan));
            }
        } catch (final AccumuloSecurityException | AccumuloException | TableNotFoundException | StoreException e) {
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
        if ((!AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME.equals(iteratorName) || store.getProperties().getEnableValidatorIterator())
                && (store.getSchema().isAggregationEnabled())) {
            try {
                addIterator(store, store.getKeyPackage()
                        .getIteratorFactory()
                        .getIteratorSetting(store, iteratorName));
            } catch (final IteratorSettingException e) {
                throw new StoreException(e.getMessage(), e);
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
        if (null != iteratorSetting) {
            try {
                store.getConnection().tableOperations().attachIterator(store.getTableName(), iteratorSetting);
            } catch (final AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
                throw new StoreException("Add iterator with Name: " + iteratorSetting.getName(), e);
            }
        }
        TableUtils.setLocalityGroups(store);
    }

    /**
     * Utility for creating and updating an Accumulo table.
     * Accumulo tables are automatically created when the Gaffer Accumulo store
     * is initialised when an instance of Graph is created.
     * <p>
     * Running this with an existing table will remove the existing iterators
     * and recreate them with the provided schema.
     * </p>
     * <p>
     * A FileGraphLibrary path must be specified as an argument.  If no path is set NoGraphLibrary will be used.
     * </p>
     * <p>
     * Usage: java -cp accumulo-store-[version]-utility.jar uk.gov.gchq.gaffer.accumulostore.utils.AddUpdateTableIterator [graphId] [pathToSchemaDirectory] [pathToStoreProperties] [pathToFileGraphLibrary]
     * </p>
     *
     * @param args [graphId] [schema directory path] [store properties path] [ file graph library path]
     * @throws Exception if the tables fails to be created/updated
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < NUM_REQUIRED_ARGS) {
            System.err.println("Wrong number of arguments. \nUsage: "
                    + "<graphId> "
                    + "<comma separated schema paths> <store properties path> "
                    + "<" + ADD_KEY + "," + REMOVE_KEY + " or " + UPDATE_KEY + "> "
                    + "<file graph library path>");
            System.exit(1);
        }

        final AccumuloProperties storeProps = AccumuloProperties.loadStoreProperties(getAccumuloPropertiesPath(args));
        if (null == storeProps) {
            throw new IllegalArgumentException("Store properties are required to create a store");
        }

        final Schema schema = Schema.fromJson(getSchemaPaths(args));

        GraphLibrary library;

        if (null == getFileGraphLibraryPathString(args)) {
            library = new NoGraphLibrary();
        } else {
            library = new FileGraphLibrary(getFileGraphLibraryPathString(args));
        }

        library.addOrUpdate(getGraphId(args), schema, storeProps);

        final String storeClass = storeProps.getStoreClass();
        if (null == storeClass) {
            throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS);
        }

        final AccumuloStore store;
        try {
            store = Class.forName(storeClass).asSubclass(AccumuloStore.class).newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
        }

        try {
            store.preInitialise(
                    getGraphId(args),
                    schema,
                    storeProps
            );
        } catch (final StoreException e) {
            throw new IllegalArgumentException("Could not initialise the store with provided arguments.", e);
        }

        if (!store.getConnection().tableOperations().exists(store.getTableName())) {
            TableUtils.createTable(store);
        }

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
        return arg[3];
    }

    private static Path getAccumuloPropertiesPath(final String[] args) {
        return Paths.get(args[2]);
    }

    private static String getGraphId(final String[] args) {
        return args[0];
    }

    private static String getFileGraphLibraryPathString(final String[] args) {
        if (args.length > 4) {
            return args[4];
        }
        return null;
    }

    private static Path[] getSchemaPaths(final String[] args) {
        final String[] pathStrs = args[1].split(",");
        final Path[] paths = new Path[pathStrs.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Paths.get(pathStrs[i]);
        }

        return paths;
    }
}
