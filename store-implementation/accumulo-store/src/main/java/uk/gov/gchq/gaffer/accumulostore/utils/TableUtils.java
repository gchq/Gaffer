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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
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
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.CoreKeyBloomFunctor;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Static utilities used in the creation and maintenance of accumulo tables.
 */
public final class TableUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableUtils.class);
    public static final String COLUMN_FAMILIES_OPTION = "columns";

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
        final String tableName = store.getTableName();
        if (null == tableName) {
            throw new AccumuloRuntimeException("Table name is required.");
        }
        final Connector connector = store.getConnection();
        if (connector.tableOperations().exists(tableName)) {
            validateTable(store, tableName, connector);
        } else {
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
        final String tableName = store.getTableName();
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

            if (store.getSchema().isAggregationEnabled()) {
                // Add Combiner iterator to table for all scopes
                LOGGER.info("Adding Aggregator iterator to table {} for all scopes", tableName);
                connector.tableOperations().attachIterator(tableName,
                        store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store));
            } else {
                LOGGER.info("Aggregator iterator has not been added to table {}", tableName);
            }

            if (store.getProperties().getEnableValidatorIterator()) {
                // Add validator iterator to table for all scopes
                final IteratorSetting itrSetting = store.getKeyPackage().getIteratorFactory().getValidatorIteratorSetting(store);
                if (null == itrSetting) {
                    LOGGER.info("Not adding Validator iterator to table {} as there are no validation functions defined in the schema", tableName);
                } else {
                    LOGGER.info("Adding Validator iterator to table {} for all scopes", tableName);
                    connector.tableOperations().attachIterator(tableName,
                            store.getKeyPackage().getIteratorFactory().getValidatorIteratorSetting(store));
                }
            } else {
                LOGGER.info("Validator iterator has not been added to table {}", tableName);
            }
        } catch (final AccumuloSecurityException | TableNotFoundException | AccumuloException | IteratorSettingException e) {
            throw new StoreException(e.getMessage(), e);
        }
        setLocalityGroups(store);
    }

    public static void setLocalityGroups(final AccumuloStore store) throws StoreException {
        final String tableName = store.getTableName();
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
        } catch (final AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
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
        return createBatchWriter(store, store.getTableName());
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
        } catch (final AccumuloException | AccumuloSecurityException e) {
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
        } catch (final AccumuloException | AccumuloSecurityException e) {
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
                    + store.getTableName(), e);
        }
    }

    private static void validateTable(final AccumuloStore store, final String tableName, final Connector connector) throws StoreException {
        final IteratorSetting requiredAggItrSetting;
        if (store.getSchema().isAggregationEnabled()) {
            try {
                requiredAggItrSetting = store.getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(store);
                if (null != requiredAggItrSetting) {
                    requiredAggItrSetting.removeOption(AccumuloStoreConstants.SCHEMA);
                    requiredAggItrSetting.removeOption(COLUMN_FAMILIES_OPTION);
                }
            } catch (final IteratorSettingException e) {
                throw new StoreException("Unable to create aggregator iterator settings", e);
            }
        } else {
            requiredAggItrSetting = null;
        }

        final IteratorSetting requiredValidatorItrSetting;
        if (store.getProperties().getEnableValidatorIterator()) {
            requiredValidatorItrSetting = store.getKeyPackage().getIteratorFactory().getValidatorIteratorSetting(store);
            if (null != requiredValidatorItrSetting) {
                requiredValidatorItrSetting.removeOption(AccumuloStoreConstants.SCHEMA);
                requiredValidatorItrSetting.removeOption(COLUMN_FAMILIES_OPTION);
            }
        } else {
            requiredValidatorItrSetting = null;
        }

        final ValidationResult validationResult = new ValidationResult();
        for (final IteratorScope iteratorScope : EnumSet.allOf(IteratorScope.class)) {
            final IteratorSetting aggItrSetting;
            final IteratorSetting validatorItrSetting;
            final IteratorSetting versioningIterSetting;
            try {
                aggItrSetting = store.getConnection().tableOperations().getIteratorSetting(tableName, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, iteratorScope);
                if (null != aggItrSetting) {
                    aggItrSetting.removeOption(AccumuloStoreConstants.SCHEMA);
                    aggItrSetting.removeOption(COLUMN_FAMILIES_OPTION);
                }
                validatorItrSetting = store.getConnection().tableOperations().getIteratorSetting(tableName, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, iteratorScope);
                if (null != validatorItrSetting) {
                    validatorItrSetting.removeOption(AccumuloStoreConstants.SCHEMA);
                    validatorItrSetting.removeOption(COLUMN_FAMILIES_OPTION);
                }
                versioningIterSetting = store.getConnection().tableOperations().getIteratorSetting(tableName, "vers", iteratorScope);
            } catch (final AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
                throw new StoreException("Unable to find iterators on the table " + tableName, e);
            }

            if (!Objects.equals(requiredAggItrSetting, aggItrSetting)) {
                validationResult.addError("Aggregator iterator for scope " + iteratorScope.name() + " is not as expected. "
                        + "Expected: " + requiredAggItrSetting + ", but found: " + aggItrSetting);
            }
            if (!Objects.equals(requiredValidatorItrSetting, validatorItrSetting)) {
                validationResult.addError("Validator iterator for scope " + iteratorScope.name() + " is not as expected. "
                        + "Expected: " + requiredValidatorItrSetting + ", but found: " + validatorItrSetting);
            }
            if (null != versioningIterSetting) {
                validationResult.addError("The versioning iterator for scope " + iteratorScope.name() + " should not be set on the table.");
            }
        }

        final Iterable<Map.Entry<String, String>> tableProps;
        try {
            tableProps = connector.tableOperations().getProperties(tableName);
        } catch (final AccumuloException | TableNotFoundException e) {
            throw new StoreException("Unable to get table properties.", e);
        }

        boolean bloomFilterEnabled = false;
        String bloomKeyFunctor = null;
        for (final Map.Entry<String, String> tableProp : tableProps) {
            if (Property.TABLE_BLOOM_ENABLED.getKey().equals(tableProp.getKey())) {
                if (Boolean.parseBoolean(tableProp.getValue())) {
                    bloomFilterEnabled = true;
                }
            } else if (Property.TABLE_BLOOM_KEY_FUNCTOR.getKey().equals(tableProp.getKey())) {
                if (null == bloomKeyFunctor || CoreKeyBloomFunctor.class.getName().equals(tableProp.getValue())) {
                    bloomKeyFunctor = tableProp.getValue();
                }
            }
        }

        if (!bloomFilterEnabled) {
            validationResult.addError("Bloom filter is not enabled. " + Property.TABLE_BLOOM_ENABLED.getKey() + " = " + bloomFilterEnabled);
        }

        if (!CoreKeyBloomFunctor.class.getName().equals(bloomKeyFunctor)) {
            validationResult.addError("Bloom key functor class is incorrect. "
                    + "Expected: " + CoreKeyBloomFunctor.class.getName() + ", but found: " + bloomKeyFunctor);
        }

        if (!validationResult.isValid()) {
            throw new StoreException("Your table " + tableName + " is configured incorrectly. "
                    + validationResult.getErrorString()
                    + "\nEither delete the table and let Gaffer create it for you or fix it manually using the Accumulo shell or the Gaffer AddUpdateTableIterator utility.");
        }
    }
}
