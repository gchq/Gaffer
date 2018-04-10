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

package uk.gov.gchq.gaffer.hbasestore.utils;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.GafferCoprocessor;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static utilities used in the creation and maintenance of HBase tables.
 * <p>
 * This class also has a main method to create and update an HBase table.
 * </p>
 */
public final class TableUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);
    private static final int NUM_REQUIRED_ARGS = 3;

    private TableUtils() {
    }

    /**
     * Utility for creating and updating an HBase table.
     * See the HBase Store README for more information on what changes to your schema you are allowed to make.
     * HBase tables are automatically created when the Gaffer HBase store is initialised when an instance of Graph is created.
     * <p>
     * Running this with an existing table will remove the existing Gaffer Coprocessor and recreate it.
     * </p>
     * <p>
     * A FileGraphLibrary path must be specified as an argument.  If no path is set NoGraphLibrary will be used.
     * </p>
     * <p>
     * Usage:
     * </p>
     * <p>
     * java -cp hbase-store-[version]-utility.jar uk.gov.gchq.gaffer.hbasestore.utils.TableUtils [graphId] [pathToSchemaDirectory] [pathToStoreProperties] [pathToFileGraphLibrary]
     * </p>
     *
     * @param args [graphId] [schema directory path] [store properties path] [ file graph library path]
     * @throws Exception if the tables fails to be created/updated
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < NUM_REQUIRED_ARGS) {
            System.err.println("Wrong number of arguments. \nUsage: "
                    + "<graphId> <schema directory path> <store properties path> <file graph library path>");
            System.exit(1);
        }

        final HBaseProperties storeProps = HBaseProperties.loadStoreProperties(getStorePropertiesPathString(args));
        if (null == storeProps) {
            throw new IllegalArgumentException("Store properties are required to create a store");
        }

        final Schema schema = Schema.fromJson(getSchemaDirectoryPath(args));

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

        final HBaseStore store;
        try {
            store = Class.forName(storeClass).asSubclass(HBaseStore.class).newInstance();
        } catch (final InstantiationException | IllegalAccessException |
                ClassNotFoundException e
                ) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
        }

        store.preInitialise(
                getGraphId(args),
                schema,
                storeProps
        );

        if (!store.getConnection().getAdmin()
                .tableExists(store.getTableName())) {
            createTable(store);
        }

        try (final Admin admin = store.getConnection().getAdmin()) {
            final TableName tableName = store.getTableName();
            if (admin.tableExists(tableName)) {
                final HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
                descriptor.removeCoprocessor(GafferCoprocessor.class.getName());
                addCoprocesssor(descriptor, store);
                admin.modifyTable(tableName, descriptor);
            } else {
                TableUtils.createTable(store);
            }
        }

    }

    private static String getGraphId(final String[] args) {
        return args[0];
    }

    private static Path getSchemaDirectoryPath(final String[] args) {
        return Paths.get(args[1]);
    }

    private static String getStorePropertiesPathString(final String[] args) {
        return args[2];
    }

    private static String getFileGraphLibraryPathString(final String[] args) {
        if (args.length > 3) {
            return args[3];
        }
        return null;
    }

    /**
     * Ensures that the table exists, otherwise it creates it and sets it up to
     * receive Gaffer data
     *
     * @param store the hbase store
     * @throws StoreException if a connection to hbase could not be created or there is a failure to create the table
     */
    public static void ensureTableExists(final HBaseStore store) throws StoreException {
        final Connection connection = store.getConnection();
        final TableName tableName = store.getTableName();
        try {
            final Admin admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                validateTable(tableName, admin);
            } else {
                try {
                    TableUtils.createTable(store);
                } catch (final Exception e) {
                    if (!admin.tableExists(tableName)) {
                        if (e instanceof StoreException) {
                            throw e;
                        } else {
                            throw new StoreException("Failed to create table " + tableName, e);
                        }
                    }
                    // If the table exists then it must have been created in a different thread.
                }
            }
        } catch (final IOException e) {
            throw new StoreException("Failed to check if table " + tableName + " exists", e);
        }
    }

    /**
     * Creates an HBase table for the given HBase store.
     *
     * @param store the hbase store
     * @throws StoreException if a connection to hbase could not be created or there is a failure to create the table
     */
    public static synchronized void createTable(final HBaseStore store)
            throws StoreException {
        final TableName tableName = store.getTableName();
        try {
            final Admin admin = store.getConnection().getAdmin();
            if (admin.tableExists(tableName)) {
                LOGGER.info("Table {} already exists", tableName);
                return;
            }
            LOGGER.info("Creating table {}", tableName);

            final HTableDescriptor htable = new HTableDescriptor(tableName);
            final HColumnDescriptor col = new HColumnDescriptor(HBaseStoreConstants.getColFam());

            // TODO: Currently there is no way to disable versions in HBase.
            // HBase have this note in their code "Allow maxVersion of 0 to be the way you say 'Keep all versions'."
            // As soon as HBase have made this update we can set the max versions number to 0.
            col.setMaxVersions(Integer.MAX_VALUE);
            htable.addFamily(col);
            addCoprocesssor(htable, store);
            admin.createTable(htable);
        } catch (final Exception e) {
            LOGGER.warn("Failed to create table {}", tableName, e);
            throw new StoreException("Failed to create table " + tableName, e);
        }

        ensureTableExists(store);
        LOGGER.info("Table {} created", tableName);
    }

    public static void deleteAllRows(final HBaseStore store, final String... auths) throws StoreException {
        final Connection connection = store.getConnection();
        try {
            if (connection.getAdmin().tableExists(store.getTableName())) {
                connection.getAdmin().flush(store.getTableName());
                final Table table = connection.getTable(store.getTableName());
                final Scan scan = new Scan();
                scan.setAuthorizations(new Authorizations(auths));
                try (ResultScanner scanner = table.getScanner(scan)) {
                    final List<Delete> deletes = new ArrayList<>();
                    for (final Result result : scanner) {
                        deletes.add(new Delete(result.getRow()));
                    }
                    table.delete(deletes);
                    connection.getAdmin().flush(store.getTableName());
                }

                try (ResultScanner scanner = table.getScanner(scan)) {
                    if (scanner.iterator().hasNext()) {
                        throw new StoreException("Some rows in table " + store.getTableName() + " failed to delete");
                    }
                }
            }
        } catch (final IOException e) {
            throw new StoreException("Failed to delete all rows in table " + store.getTableName(), e);
        }
    }

    public static void dropTable(final HBaseStore store) throws StoreException {
        dropTable(store.getConnection(), store.getTableName());
    }

    public static void dropTable(final Connection connection, final TableName tableName) throws StoreException {
        try {
            final Admin admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                LOGGER.info("Dropping table: {}", tableName.getNameAsString());
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }
        } catch (final IOException e) {
            throw new StoreException("Failed to drop table " + tableName, e);
        }
    }

    public static void dropAllTables(final Connection connection) throws StoreException {
        try {
            final Admin admin = connection.getAdmin();
            for (final TableName tableName : admin.listTableNames()) {
                dropTable(connection, tableName);
            }
        } catch (final IOException e) {
            throw new StoreException(e);
        }
    }

    private static void addCoprocesssor(final HTableDescriptor htable, final HBaseStore store) throws IOException {
        final String schemaJson = StringUtil.escapeComma(
                Bytes.toString(store.getSchema().toCompactJson()));
        final Map<String, String> options = new HashMap<>(1);
        options.put(HBaseStoreConstants.SCHEMA, schemaJson);
        htable.addCoprocessor(GafferCoprocessor.class.getName(), store.getProperties().getDependencyJarsHdfsDirPath(), Coprocessor.PRIORITY_USER, options);
    }

    private static void validateTable(final TableName tableName, final Admin admin) throws StoreException {
        final ValidationResult validationResult = new ValidationResult();

        final HTableDescriptor descriptor;
        try {
            descriptor = admin.getTableDescriptor(tableName);
        } catch (final IOException e) {
            throw new StoreException("Unable to look up the table coprocessors", e);
        }

        final HColumnDescriptor col = descriptor.getFamily(HBaseStoreConstants.getColFam());
        if (null == col) {
            validationResult.addError("The Gaffer element 'e' column family does not exist");
        } else if (Integer.MAX_VALUE != col.getMaxVersions()) {
            validationResult.addError("The maximum number of versions should be set to " + Integer.MAX_VALUE);
        }

        if (!descriptor.hasCoprocessor(GafferCoprocessor.class.getName())) {
            validationResult.addError("Missing coprocessor: " + GafferCoprocessor.class.getName());
        }

        if (!validationResult.isValid()) {
            throw new StoreException("Your table " + tableName + " is configured incorrectly. "
                    + validationResult.getErrorString()
                    + "\nEither delete the table and let Gaffer create it for you or fix it manually using the HBase shell or the Gaffer TableUtils utility.");
        }
    }
}
