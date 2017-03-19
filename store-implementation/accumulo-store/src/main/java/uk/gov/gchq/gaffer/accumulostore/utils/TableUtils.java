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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloRuntimeException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.store.StoreException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Static utilities used in the creation and maintenance of accumulo tables.
 */
public final class TableUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);

    private TableUtils() {
    }

    /**
     * Ensures that the table exists, otherwise it creates it and sets it up to
     * receive Gaffer data
     *
     * @param store the accumulo store
     * @throws StoreException if a connection to accumulo could not be created or there is a failure to create a table/iterator
     */
    public static void ensureTableExists(final AccumuloStore store) throws StoreException {
        final String tableName = store.getProperties().getTable();
        if (null == tableName) {
            throw new AccumuloRuntimeException("Table name is required.");
        }
        final Connector connector = store.getConnection();
        if (!connector.tableOperations().exists(tableName)) {
            try {
                TableUtils.createTable(store);
            } catch (final TableExistsException e) {
                // The method to create a table is synchronised, if you are using the same store only through one client in one JVM you shouldn't get here
                // Someone else got there first, never mind...
            }
        }
    }

    /**
     * Creates a table for Gaffer data and enables the correct Bloom filter;
     * removes the versioning iterator and adds an aggregator Iterator the
     * {@link org.apache.accumulo.core.iterators.user.AgeOffFilter} for the
     * specified time period.
     *
     * @param store the accumulo store
     * @throws StoreException       failure to create accumulo connection or add iterator settings
     * @throws TableExistsException failure to create table
     */
    public static synchronized void createTable(final AccumuloStore store)
            throws StoreException, TableExistsException {
        // Create table
        final String tableName = store.getProperties().getTable();
        if (null == tableName) {
            throw new AccumuloRuntimeException("Table name is required.");
        }
        final Connector connector = store.getConnection();
        if (connector.tableOperations().exists(tableName)) {
            LOGGER.info("Table {} exists, not creating", tableName);
            return;
        }
        try {
            LOGGER.info("Creating table {} as user {}", tableName, connector.whoami());
            connector.tableOperations().create(tableName);
            final String repFactor = store.getProperties().getTableFileReplicationFactor();
            if (null != repFactor) {
                LOGGER.info("Table file replication set to {} on table {}", repFactor, tableName);
                connector.tableOperations().setProperty(tableName, Property.TABLE_FILE_REPLICATION.getKey(), repFactor);
            }

            // Enable Bloom filters using ElementFunctor
            LOGGER.info("Enabling Bloom filter on table {}", tableName);
            connector.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), "true");
            connector.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(),
                    store.getKeyPackage().getKeyFunctor().getClass().getName());

            // Remove versioning iterator from table for all scopes
            LOGGER.info("Removing versioning iterator from table {}", tableName);
            final EnumSet<IteratorScope> iteratorScopes = EnumSet.allOf(IteratorScope.class);
            connector.tableOperations().removeIterator(tableName, "vers", iteratorScopes);

            if (store.getSchema().hasAggregators()) {
                // Add Combiner iterator to table for all scopes
                LOGGER.info("Adding Aggregator iterator to table {} for all scopes", tableName);
                connector.tableOperations().attachIterator(tableName,
                        store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
            } else {
                LOGGER.info("Aggregator iterator has not been added to table {}", tableName);
            }

            if (store.getProperties().getEnableValidatorIterator()) {
                // Add validator iterator to table for all scopes
                LOGGER.info("Adding Validator iterator to table {} for all scopes", tableName);
                connector.tableOperations().attachIterator(tableName,
                        store.getKeyPackage().getIteratorFactory().getValidatorIteratorSetting(store));
            } else {
                LOGGER.info("Validator iterator has not been added to table {}", tableName);
            }
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException | IteratorSettingException e) {
            throw new StoreException(e.getMessage(), e);
        }
        setLocalityGroups(store);
    }

    public static void setLocalityGroups(final AccumuloStore store) throws StoreException {
        final String tableName = store.getProperties().getTable();
        Map<String, Set<Text>> localityGroups =
                new HashMap<>();
        for (final String group : store.getSchema().getGroups()) {
            HashSet<Text> localityGroup = new HashSet<>();
            localityGroup.add(new Text(group));
            localityGroups.put(group, localityGroup);
        }
        LOGGER.info("Setting locality groups on table {}", tableName);
        try {
            store.getConnection().tableOperations().setLocalityGroups(tableName, localityGroups);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * Creates a {@link BatchWriter}
     * <p>
     *
     * @param store the accumulo store
     * @return A new BatchWriter with the settings defined in the
     * gaffer.accumulostore properties
     * @throws StoreException if the table could not be found or other table issues
     */
    public static BatchWriter createBatchWriter(final AccumuloStore store) throws StoreException {
        return createBatchWriter(store, store.getProperties().getTable());
    }

    /**
     * Creates a connection to an accumulo instance using the provided
     * parameters
     *
     * @param instanceName the instance name
     * @param zookeepers   the zoo keepers
     * @param userName     the user name
     * @param password     the password
     * @return A connection to an accumulo instance
     * @throws StoreException failure to create an accumulo connection
     */
    public static Connector getConnector(final String instanceName, final String zookeepers, final String userName,
                                         final String password) throws StoreException {
        final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        try {
            return instance.getConnector(userName, new PasswordToken(password));
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new StoreException("Failed to create accumulo connection", e);
        }
    }

    /**
     * Returns the {@link org.apache.accumulo.core.security.Authorizations} of
     * the current user
     *
     * @param connection the connection to an accumulo instance
     * @return The accumulo Authorisations of the current user specified in the properties file
     * @throws StoreException if the table could not be found or other table/security issues
     */
    public static Authorizations getCurrentAuthorizations(final Connector connection) throws StoreException {
        try {
            return connection.securityOperations().getUserAuthorizations(connection.whoami());
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    /**
     * Creates a {@link org.apache.accumulo.core.client.BatchWriter} for the
     * specified table
     * <p>
     *
     * @param store     the accumulo store
     * @param tableName the table name
     * @return A new BatchWriter with the settings defined in the
     * gaffer.accumulostore properties
     * @throws StoreException if the table could not be found or other table issues
     */

    private static BatchWriter createBatchWriter(final AccumuloStore store, final String tableName)
            throws StoreException {
        final BatchWriterConfig batchConfig = new BatchWriterConfig();
        batchConfig.setMaxMemory(store.getProperties().getMaxBufferSizeForBatchWriterInBytes());
        batchConfig.setMaxLatency(store.getProperties().getMaxTimeOutForBatchWriterInMilliseconds(),
                TimeUnit.MILLISECONDS);
        batchConfig.setMaxWriteThreads(store.getProperties().getNumThreadsForBatchWriter());
        try {
            return store.getConnection().createBatchWriter(tableName, batchConfig);
        } catch (final TableNotFoundException e) {
            throw new StoreException("Table not set up! Use table gaffer.accumulostore.utils to create the table"
                    + store.getProperties().getTable(), e);
        }
    }
}
