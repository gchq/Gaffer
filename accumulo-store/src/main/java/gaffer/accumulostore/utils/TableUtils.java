/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.utils;

import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.AccumuloProperties;
import gaffer.accumulostore.AccumuloStore;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.store.StoreException;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Static utilities used in the creation and maintenance of accumulo tables.
 */
public class TableUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);

    private TableUtils() {
    }

    /**
     * Ensures that the table exists, otherwise it creates it and sets it up to receive Gaffer data
     *
     * @param store
     * @throws org.apache.accumulo.core.client.AccumuloException
     */
    public static void ensureTableExists(final AccumuloStore store) throws AccumuloException {
        final Connector conn;
        try {
            conn = store.getConnection();
        } catch (StoreException e) {
            throw new AccumuloException(e);
        }
        if (!conn.tableOperations().exists(store.getProperties().getTable())) {
            try {
                TableUtils.createTable(store);
            } catch (TableExistsException e) {
                // Someone else got there first, never mind...
            } catch (IteratorSettingException e) {
                throw new AccumuloException(e);
            }
        }
    }

    /**
     * Creates a table for Gaffer data and enables the correct Bloom filter; removes the versioning
     * iterator and adds an aggregator Iterator the {@link org.apache.accumulo.core.iterators.user.AgeOffFilter} for the specified time period.
     *
     * @param store
     * @throws org.apache.accumulo.core.client.AccumuloException
     * @throws gaffer.accumulostore.key.exception.IteratorSettingException
     * @throws org.apache.accumulo.core.client.TableExistsException
     */
    public static void createTable(final AccumuloStore store)
            throws AccumuloException, IteratorSettingException, TableExistsException {
        // Create table
        final Connector connector;
        try {
            connector = store.getConnection();
        } catch (StoreException e) {
            throw new AccumuloException(e);
        }
        String tableName = store.getProperties().getTable();
        try {
            connector.tableOperations().create(tableName);
            String repFactor = store.getProperties().getTableFileReplicationFactor();
            if(null !=repFactor) {
            	connector.tableOperations().setProperty(tableName, Property.TABLE_FILE_REPLICATION.getKey(), repFactor);
        	}
            // Enable Bloom filters using ElementFunctor
            LOGGER.info("Enabling Bloom filter on table");
            connector.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), "true");
            connector.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), store.getKeyPackage().getKeyFunctor().getClass().getName());
            LOGGER.info("Bloom filter enabled");

            // Remove versioning iterator from table for all scopes
            LOGGER.info("Removing versioning iterator");
            EnumSet<IteratorScope> iteratorScopes = EnumSet.allOf(IteratorScope.class);
            connector.tableOperations().removeIterator(tableName, "vers", iteratorScopes);
            LOGGER.info("Versioning iterator removed");

            // Add Combiner iterator to table for all scopes
            LOGGER.info("Combiner iterator to table for all scopes");
            connector.tableOperations().attachIterator(tableName, store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
            LOGGER.info("Combiner iterator to table for all scopes");

            // Add age off iterator to table for all scopes
            LOGGER.info("Adding age off iterator to table for all scopes");
            final Long ageOfTimeInMils = store.getProperties().getAgeOffTimeInDays() * 24 * 60 * 60 * 1000L;
            connector.tableOperations().attachIterator(tableName, getAgeOffIteratorSetting(ageOfTimeInMils));
        } catch (AccumuloSecurityException | TableNotFoundException e) {
            throw new AccumuloException(e);
        }
        LOGGER.info("Added age off iterator to table for all scopes");

        try {
            addUpdateUtilsTable(store);
        } catch (TableUtilException e) {
            throw new AccumuloException(e);
        }
    }

    /**
     * Creates a {@link BatchWriter}
     *
     * @param store
     * @return A new BatchWriter with the settings defined in the gaffer.accumulostore properties
     * @throws TableUtilException
     */
    public static BatchWriter createBatchWriter(final AccumuloStore store) throws TableUtilException {
        return createBatchWriter(store, store.getProperties().getTable());
    }

    /**
     * Returns the map containing all the information needed to create a new instance of the accumulo gaffer.accumulostore
     *
     * @param properties
     * @return A MapWritable containing all the required information to construct an accumulo gaffer.accumulostore instance
     * @throws TableUtilException
     */
    public static MapWritable getStoreConstructorInfo(final AccumuloProperties properties) throws TableUtilException {
        Connector connection = getConnector(properties.getInstanceName(), properties.getZookeepers(), properties.getUserName(), properties.getPassword());
        BatchScanner scanner;
        try {
            scanner = connection.createBatchScanner(Constants.GAFFER_UTILS_TABLE, getCurrentAuthorizations(connection), properties.getThreadsForBatchScanner());
        } catch (TableNotFoundException e) {
            throw new TableUtilException(e);
        }
        scanner.setRanges(Collections.singleton(getTableSetupRange(properties.getTable())));
        Iterator<Entry<Key, Value>> iter = scanner.iterator();
        if (iter.hasNext()) {
            return getSchemasFromValue(iter.next().getValue());
        } else {
            return null;
        }
    }

    /**
     * Creates a connection to an accumulo instance using the provided parameters
     *
     * @param instanceName
     * @param zookeepers
     * @param userName
     * @param password
     * @return A connection to an accumulo instance
     * @throws TableUtilException
     */
    public static Connector getConnector(final String instanceName, final String zookeepers, final String userName, final String password) throws TableUtilException {
        final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        try {
            return instance.getConnector(userName, new PasswordToken(password));
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new TableUtilException("Failed to create accumulo connection", e);
        }
    }

    /**
     * Returns the {@link org.apache.accumulo.core.security.Authorizations} of the current user
     *
     * @param connection
     * @return The accumulo Authorisations of the current user specified in the properties file
     * @throws TableUtilException
     */
    public static Authorizations getCurrentAuthorizations(final Connector connection) throws TableUtilException {
        try {
            return connection.securityOperations().getUserAuthorizations(connection.whoami());
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new TableUtilException(e.getMessage(), e);
        }
    }

    private static void ensureUtilsTableExists(final AccumuloStore store) throws TableUtilException {
        final Connector conn;
        try {
            conn = store.getConnection();
        } catch (StoreException e) {
            throw new TableUtilException(e);
        }
        if (!conn.tableOperations().exists(Constants.GAFFER_UTILS_TABLE)) {
            try {
                conn.tableOperations().create(Constants.GAFFER_UTILS_TABLE);
            } catch (TableExistsException e) {
                // Someone else got there first, never mind...
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new TableUtilException("Failed to create : " + Constants.GAFFER_UTILS_TABLE + " table", e);
            }
        }
    }

    public static void addUpdateUtilsTable(final AccumuloStore store) throws TableUtilException {
        ensureUtilsTableExists(store);
        final BatchWriter writer = createBatchWriter(store, Constants.GAFFER_UTILS_TABLE);
        final Key key = new Key(store.getProperties().getTable().getBytes(), Constants.EMPTY_BYTES, Constants.EMPTY_BYTES, Constants.EMPTY_BYTES, Long.MAX_VALUE);
        final Mutation m = new Mutation(key.getRow());
        m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), getValueFromSchemas(store.getDataSchema(), store.getStoreSchema(), store.getKeyPackage()));
        try {
            writer.addMutation(m);
        } catch (MutationsRejectedException e) {
            LOGGER.error("Failed to create an accumulo gaffer.accumulostore.key mutation");
        }
    }

    /**
     * Creates a {@link org.apache.accumulo.core.client.BatchWriter} for the specified table
     *
     * @param store
     * @param tableName
     * @return A new BatchWriter with the settings defined in the gaffer.accumulostore properties
     * @throws TableUtilException
     */

    private static BatchWriter createBatchWriter(final AccumuloStore store, final String tableName) throws TableUtilException {
        final BatchWriterConfig batchConfig = new BatchWriterConfig();
        batchConfig.setMaxMemory(store.getProperties().getMaxBufferSizeForBatchWriterInBytes());
        batchConfig.setMaxLatency(store.getProperties().getMaxTimeOutForBatchWriterInMilliseconds(), TimeUnit.MILLISECONDS);
        batchConfig.setMaxWriteThreads(store.getProperties().getNumThreadsForBatchWriter());
        try {
            return store.getConnection().createBatchWriter(tableName, batchConfig);
        } catch (TableNotFoundException e) {
            throw new TableUtilException("Table not set up! Use table gaffer.accumulostore.utils to create the table" + store.getProperties().getTable(), e);
        } catch (StoreException e) {
            throw new TableUtilException(e);
        }
    }

    /**
     * Returns an {@link org.apache.accumulo.core.client.IteratorSetting} that specifies the age off iterator.
     *
     * @param ageOffTimeInMilliseconds
     * @return An iterator setting describing an age off iterator
     */
    private static IteratorSetting getAgeOffIteratorSetting(final long ageOffTimeInMilliseconds) {
        return new IteratorSettingBuilder(Constants.AGE_OFF_ITERATOR_PRIORITY, "ageoff", AgeOffFilter.class)
                .option("ttl", "" + ageOffTimeInMilliseconds)
                .build();
    }

    private static Range getTableSetupRange(final String table) {
        return new Range(getTableSetupKey(table.getBytes(), false), getTableSetupKey(table.getBytes(), true));
    }

    private static Key getTableSetupKey(final byte[] serialisedVertex, final boolean endKey) {
        final byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 1);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length);
        }
        return new Key(key, Constants.EMPTY_BYTES, Constants.EMPTY_BYTES, Constants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private static Value getValueFromSchemas(final DataSchema dataSchema, final StoreSchema storeSchema, final AccumuloKeyPackage keyPackage) throws TableUtilException {
        final MapWritable map = new MapWritable();
        map.put(Constants.DATA_SCHEMA_KEY, new BytesWritable(dataSchema.toJson(false)));
        map.put(Constants.STORE_SCHEMA_KEY, new BytesWritable(storeSchema.toJson(false)));
        map.put(Constants.KEY_PACKAGE_KEY, new BytesWritable(keyPackage.getClass().getName().getBytes()));
        return new Value(WritableUtils.toByteArray(map));
    }

    private static MapWritable getSchemasFromValue(final Value value) throws TableUtilException {
        MapWritable map = new MapWritable();
        try {
            map.readFields(new DataInputStream(new ByteArrayInputStream(value.get())));
        } catch (IOException e) {
            throw new TableUtilException("Failed to read map writable from value", e);
        }
        return map;
    }

}