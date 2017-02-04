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

package uk.gov.gchq.gaffer.hbasestore.utils;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.io.IOException;
import java.util.Map;

/**
 * Static utilities used in the creation and maintenance of hbase tables.
 */
public final class TableUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);

    private TableUtils() {
    }

    /**
     * Ensures that the table exists, otherwise it creates it and sets it up to
     * receive Gaffer data
     *
     * @param store the hbase store
     * @throws StoreException if a connection to hbase could not be created or there is a failure to create a table/iterator
     */
    public static void ensureTableExists(final HBaseStore store) throws StoreException {
        final Connection connection = store.getConnection();
        final TableName tableName = store.getProperties().getTable();
        try {
            final Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                TableUtils.createTable(store);
            }
        } catch (final IOException e) {
            // The method to create a table is synchronised, if you are using the same store only through one client in one JVM you shouldn't get here
            // Someone else got there first, never mind...
        }
    }

    public static Table getTable(final HBaseStore store) throws StoreException {
        final Connection connection = store.getConnection();
        final TableName tableName = store.getProperties().getTable();
        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    public static synchronized void createTable(final HBaseStore store)
            throws StoreException {
        // Create table
        final Connection connection = store.getConnection();
        final TableName tableName = store.getProperties().getTable();
        try {
            final Admin admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                LOGGER.info("Table {} exists, not creating", tableName);
                return;
            }
            LOGGER.info("Creating table {} as user {}", tableName, store.getProperties().getUserName());

            HTableDescriptor htable = new HTableDescriptor(tableName);
            for (final String group : store.getSchema().getEntityGroups()) {
                htable.addFamily(new HColumnDescriptor(Bytes.toBytes(group)));
            }
            for (final String group : store.getSchema().getEdgeGroups()) {
                htable.addFamily(new HColumnDescriptor(Bytes.toBytes(group)));
            }
            // TODO add coprocessors:
            //htable.addCoprocessor(VisibilityController.class.getName());
            //            htable.addCoprocessor()
            admin.createTable(htable);


//            final String repFactor = store.getProperties().getTableFileReplicationFactor();
//            if (null != repFactor) {
//                LOGGER.info("Table file replication set to {} on table {}", repFactor, tableName);
//                connection.tableOperations().setProperty(tableName, Property.TABLE_FILE_REPLICATION.getKey(), repFactor);
//            }

            // Enable Bloom filters using ElementFunctor
//            LOGGER.info("Enabling Bloom filter on table {}", tableName);
//            connection.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), "true");
//            connection.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(),
//                    store.getKeyPackage().getKeyFunctor().getClass().getName());

            // Remove versioning iterator from table for all scopes
//            LOGGER.info("Removing versioning iterator from table {}", tableName);
//            final EnumSet<IteratorScope> iteratorScopes = EnumSet.allOf(IteratorScope.class);
//            connection.tableOperations().removeIterator(tableName, "vers", iteratorScopes);

            if (schemaContainsAggregators(store.getSchema())) {
                // Add Combiner iterator to table for all scopes
                LOGGER.info("Adding Aggregator iterator to table {} for all scopes", tableName);
//                connection.tableOperations().attachIterator(tableName,
//                        store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
            } else {
                LOGGER.info("Aggregator iterator has not been added to table {}", tableName);
            }

            if (store.getProperties().getEnableValidatorIterator()) {
                // Add validator iterator to table for all scopes
                LOGGER.info("Adding Validator iterator to table {} for all scopes", tableName);
//                connection.tableOperations().attachIterator(tableName,
//                        store.getKeyPackage().getIteratorFactory().getValidatorIteratorSetting(store));
            } else {
                LOGGER.info("Validator iterator has not been added to table {}", tableName);
            }
        } catch (Throwable e) {
            throw new StoreException(e.getMessage(), e);
        }
        //setLocalityGroups(store);
    }

//    public static void setLocalityGroups(final HBaseStore store) throws StoreException {
//        final String tableName = store.getProperties().getTable();
//        Map<String, Set<Text>> localityGroups =
//                new HashMap<>();
//        for (final String entityGroup : store.getSchema().getEntityGroups()) {
//            HashSet<Text> localityGroup = new HashSet<>();
//            localityGroup.add(new Text(entityGroup));
//            localityGroups.put(entityGroup, localityGroup);
//        }
//        for (final String edgeGroup : store.getSchema().getEdgeGroups()) {
//            HashSet<Text> localityGroup = new HashSet<>();
//            localityGroup.add(new Text(edgeGroup));
//            localityGroups.put(edgeGroup, localityGroup);
//        }
//        LOGGER.info("Setting locality groups on table {}", tableName);
//        try {
//            store.getConnection().tableOperations().setLocalityGroups(tableName, localityGroups);
//        } catch (HBaseException | HBaseSecurityException | TableNotFoundException e) {
//            throw new StoreException(e.getMessage(), e);
//        }
//    }

//    /**
//     * Creates a {@link BatchWriter}
//     * <p>
//     *
//     * @param store the hbase store
//     * @return A new BatchWriter with the settings defined in the
//     * gaffer.hbasestore properties
//     * @throws StoreException if the table could not be found or other table issues
//     */
//    public static BatchWriter createBatchWriter(final HBaseStore store) throws StoreException {
//        return createBatchWriter(store, store.getProperties().getTable());
//    }

    /**
     * Creates a connection to an hbase instance using the provided
     * parameters
     *
     * @param instanceName the instance name
     * @param zookeepers   the zoo keepers
     * @param userName     the user name
     * @param password     the password
     * @return A connection to an hbase instance
     * @throws StoreException failure to create an hbase connection
     */
    public static Connection getConnection(final String instanceName, final String zookeepers, final String userName,
                                           final String password) throws StoreException {

//        final Configuration conf = null;
//        try {
//            return ConnectionFactory.createConnection(conf);
//        } catch (IOException e) {
//            throw new StoreException(e);
//        }
        // TODO: create connection
        return null;
    }

//    /**
//     * Returns the {@link org.apache.hbase.core.security.Authorizations} of
//     * the current user
//     *
//     * @param connection the connection to an hbase instance
//     * @return The hbase Authorisations of the current user specified in the properties file
//     * @throws StoreException if the table could not be found or other table/security issues
//     */
//    public static Authorizations getCurrentAuthorizations(final Connection connection) throws StoreException {
//        try {
//            return connection.securityOperations().getUserAuthorizations(connection.whoami());
//        } catch (HBaseException | HBaseSecurityException e) {
//            throw new StoreException(e.getMessage(), e);
//        }
//    }

    /**
     * Checks the given {@link uk.gov.gchq.gaffer.store.schema.Schema} and determines
     * whether the types specified by the schema contain aggregators.
     *
     * @param schema the schema
     * @return {@code true} if the schema contains aggregators, otherwise {@code false}
     */
    public static boolean schemaContainsAggregators(final Schema schema) {
        boolean schemaContainsAggregators = false;

        final Map<String, TypeDefinition> types = schema.getTypes();

        for (final TypeDefinition type : types.values()) {
            if (null != type.getAggregateFunction()) {
                schemaContainsAggregators = true;
            }
        }

        return schemaContainsAggregators;
    }

//    /**
//     * Creates a {@link org.apache.hbase.core.client.BatchWriter} for the
//     * specified table
//     * <p>
//     *
//     * @param store     the hbase store
//     * @param tableName the table name
//     * @return A new BatchWriter with the settings defined in the
//     * gaffer.hbasestore properties
//     * @throws StoreException if the table could not be found or other table issues
//     */
//
//    private static BatchWriter createBatchWriter(final HBaseStore store, final String tableName)
//            throws StoreException {
//        final BatchWriterConfig batchConfig = new BatchWriterConfig();
//        batchConfig.setMaxMemory(store.getProperties().getMaxBufferSizeForBatchWriterInBytes());
//        batchConfig.setMaxLatency(store.getProperties().getMaxTimeOutForBatchWriterInMilliseconds(),
//                TimeUnit.MILLISECONDS);
//        batchConfig.setMaxWriteThreads(store.getProperties().getNumThreadsForBatchWriter());
//        try {
//            return store.getConnection().createBatchWriter(tableName, batchConfig);
//        } catch (final TableNotFoundException e) {
//            throw new StoreException("Table not set up! Use table gaffer.hbasestore.utils to create the table"
//                    + store.getProperties().getTable(), e);
//        }
//    }
}
