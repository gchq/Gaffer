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

package uk.gov.gchq.gaffer.accumulostore;

import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * An {@code AccumuloProperties} contains specific configuration information for the
 * {@link uk.gov.gchq.gaffer.accumulostore.AccumuloStore}, such as database connection strings. It wraps
 * {@link uk.gov.gchq.gaffer.data.element.Properties} and lazy loads the all properties from
 * a file when first used.
 */
public class AccumuloProperties extends StoreProperties {

    public static final String KEY_PACKAGE_CLASS = "gaffer.store.accumulo.keypackage.class";
    public static final String INSTANCE_NAME = "accumulo.instance";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    /**
     * @deprecated use a graphId.
     */
    @Deprecated
    public static final String TABLE = "accumulo.table";
    public static final String USER = "accumulo.user";
    public static final String PASSWORD = "accumulo.password";
    public static final String THREADS_FOR_BATCH_SCANNER = "accumulo.batchScannerThreads";
    public static final String MAX_ENTRIES_FOR_BATCH_SCANNER = "accumulo.entriesForBatchScanner";
    public static final String CLIENT_SIDE_BLOOM_FILTER_SIZE = "accumulo.clientSideBloomFilterSize";
    public static final String FALSE_POSITIVE_RATE = "accumulo.falsePositiveRate";
    public static final String MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR = "accumulo.maxBloomFilterToPassToAnIterator";
    public static final String MAX_BUFFER_SIZE_FOR_BATCH_WRITER = "accumulo.maxBufferSizeForBatchWriterInBytes";
    public static final String MAX_TIME_OUT_FOR_BATCH_WRITER = "accumulo.maxTimeOutForBatchWriterInMilliseconds";
    public static final String NUM_THREADS_FOR_BATCH_WRITER = "accumulo.numThreadsForBatchWriter";
    public static final String TABLE_REPLICATION_FACTOR = "accumulo.file.replication";
    public static final String ENABLE_VALIDATOR_ITERATOR = "gaffer.store.accumulo.enable.validator.iterator";
    public static final String HDFS_SKIP_PERMISSIONS = "accumulostore.operation.hdfs.skip_permissions";

    // default values
    private static final String NUM_THREADS_FOR_BATCH_WRITER_DEFAULT = "10";
    private static final String MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT = "50000";
    private static final String CLIENT_SIDE_BLOOM_FILTER_SIZE_DEFAULT = "838860800";
    private static final String FALSE_POSITIVE_RATE_DEFAULT = "0.0002";
    private static final String MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR_DEFAULT = "8388608";
    private static final String MAX_BUFFER_SIZE_FOR_BATCH_WRITER_DEFAULT = "100000000";
    private static final String MAX_TIME_OUT_FOR_BATCH_WRITER_DEFAULT = "1000";
    private static final String THREADS_FOR_BATCH_SCANNER_DEFAULT = "10";
    public static final String ENABLE_VALIDATOR_ITERATOR_DEFAULT = "true";

    public AccumuloProperties() {
        super(AccumuloStore.class);
    }

    public AccumuloProperties(final Path propFileLocation) {
        super(propFileLocation, AccumuloStore.class);
    }

    public static AccumuloProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, AccumuloProperties.class);
    }

    public static AccumuloProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, AccumuloProperties.class);
    }

    public static AccumuloProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, AccumuloProperties.class);
    }

    @Override
    public AccumuloProperties clone() {
        return (AccumuloProperties) super.clone();
    }

    /**
     * Sets the number of threads that should be used for the Accumulo batch
     * writers.
     *
     * @param numThreadsForBatchWriter The number of concurrent threads to use in the batch writer.
     */
    public void setNumThreadsForBatchWriter(final String numThreadsForBatchWriter) {
        set(NUM_THREADS_FOR_BATCH_WRITER, numThreadsForBatchWriter);
    }

    /**
     * Sets the time out/latency that should be used for the Accumulo batch
     * writers.
     *
     * @param maxTimeOutForBatchWriterInMilliseconds The timeout to use on the batch writer.
     */
    public void setMaxTimeOutForBatchWriterInMilliseconds(final String maxTimeOutForBatchWriterInMilliseconds) {
        set(MAX_TIME_OUT_FOR_BATCH_WRITER, maxTimeOutForBatchWriterInMilliseconds);
    }

    /**
     * Sets the memory buffer size that should be used for the Accumulo batch
     * writers.
     *
     * @param maxBufferSizeForBatchWriterInBytes The buffer size in bytes to use in the batch writer.
     */
    public void setMaxBufferSizeForBatchWriterInBytes(final String maxBufferSizeForBatchWriterInBytes) {
        set(MAX_BUFFER_SIZE_FOR_BATCH_WRITER, maxBufferSizeForBatchWriterInBytes);
    }

    /**
     * Gets the number of threads that should be used for the Accumulo batch
     * writers.
     *
     * @return The number of concurrent threads to use in the batch writer.
     */
    public int getNumThreadsForBatchWriter() {
        return Integer.parseInt(get(NUM_THREADS_FOR_BATCH_WRITER, NUM_THREADS_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the time out/latency that should be used for the Accumulo batch
     * writers.
     *
     * @return The timeout to use on the batch writer.
     */
    public Long getMaxTimeOutForBatchWriterInMilliseconds() {
        return Long.parseLong(get(MAX_TIME_OUT_FOR_BATCH_WRITER, MAX_TIME_OUT_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the memory buffer size that should be used for the Accumulo batch
     * writers.
     *
     * @return The buffer size in bytes to use in the batch writer.
     */
    public Long getMaxBufferSizeForBatchWriterInBytes() {
        return Long.parseLong(get(MAX_BUFFER_SIZE_FOR_BATCH_WRITER, MAX_BUFFER_SIZE_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the list of Zookeeper servers.
     *
     * @return A comma separated list of Zookeeper servers.
     */
    public String getZookeepers() {
        return get(ZOOKEEPERS);
    }

    /**
     * Sets the list of Zookeeper servers.
     *
     * @param zookeepers the list of Zookeeper servers.
     */
    public void setZookeepers(final String zookeepers) {
        set(ZOOKEEPERS, zookeepers);
    }

    /**
     * Gets the Accumulo instance name.
     *
     * @return Return the instance name of Accumulo set in the properties file.
     */
    public String getInstance() {
        return get(INSTANCE_NAME);
    }

    /**
     * Sets the Accumulo instance name.
     *
     * @param instance the Accumulo instance name.
     */
    public void setInstance(final String instance) {
        set(INSTANCE_NAME, instance);
    }

    /**
     * Gets the particular table name.
     *
     * @return The Accumulo table to use as set in the properties file.
     * @deprecated use {@link AccumuloStore#getTableName}.
     */
    @Deprecated
    public String getTable() {
        return get(TABLE);
    }

    /**
     * Sets the table name.
     *
     * @param tableName the table name.
     * @deprecated use a graphId.
     */
    @Deprecated
    public void setTable(final String tableName) {
        set(TABLE, tableName);
    }

    /**
     * Gets the configured Accumulo user.
     *
     * @return Get the configured accumulo user.
     */
    public String getUser() {
        return get(USER);
    }

    /**
     * Sets the configured Accumulo user.
     *
     * @param user the configured Accumulo user.
     */
    public void setUser(final String user) {
        set(USER, user);
    }

    /**
     * Gets the password for the Accumulo user.
     *
     * @return the password for the configured Accumulo user.
     */
    public String getPassword() {
        return get(PASSWORD);
    }

    /**
     * Sets the password to use for the Accumulo user.
     *
     * @param password the password to use for the Accumulo user.
     */
    public void setPassword(final String password) {
        set(PASSWORD, password);
    }

    /**
     * Gets the number of threads to use in the batch scanner.
     *
     * @return An integer representing the number of threads to use in the batch
     * scanner.
     */
    public int getThreadsForBatchScanner() {
        return Integer.parseInt(get(THREADS_FOR_BATCH_SCANNER, THREADS_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Sets the number of threads to use in the batch scanner.
     *
     * @param threadsForBatchScanner the number of threads to use in the batch scanner.
     */
    public void setThreadsForBatchScanner(final String threadsForBatchScanner) {
        set(THREADS_FOR_BATCH_SCANNER, threadsForBatchScanner);
    }

    /**
     * Gets the max number of items that should be read into the scanner at any
     * one time.
     *
     * @return An integer representing the max number of items that should be
     * read into the scanner at any one time.
     */
    public int getMaxEntriesForBatchScanner() {
        return Integer.parseInt(get(MAX_ENTRIES_FOR_BATCH_SCANNER, MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Sets the max number of items that should be read into the scanner at any
     * one time.
     *
     * @param maxEntriesForBatchScanner the max number of items that should be read into the scanner at any one time.
     */
    public void setMaxEntriesForBatchScanner(final String maxEntriesForBatchScanner) {
        set(MAX_ENTRIES_FOR_BATCH_SCANNER, maxEntriesForBatchScanner);
    }

    /**
     * Gets the size that should be used for the creation of bloom filters on the
     * client side.
     *
     * @return An integer representing the size that should be used for the
     * creation of bloom filters on the client side.
     */
    public int getClientSideBloomFilterSize() {
        return Integer.parseInt(get(CLIENT_SIDE_BLOOM_FILTER_SIZE, CLIENT_SIDE_BLOOM_FILTER_SIZE_DEFAULT));
    }

    /**
     * Sets the size that should be used for the creation of bloom filters on the
     * client side.
     *
     * @param clientSideBloomFilterSize the size that should be used for the creation of bloom filters on the client side.
     */
    public void setClientSideBloomFilterSize(final String clientSideBloomFilterSize) {
        set(CLIENT_SIDE_BLOOM_FILTER_SIZE, clientSideBloomFilterSize);
    }

    /**
     * Gets the allowable rate of false positives for bloom filters (Generally
     * the higher the value the faster the filter).
     *
     * @return A number representing the rate of false positives for bloom
     * filters (Generally the higher the value the faster the filter).
     */
    public double getFalsePositiveRate() {
        return Double.parseDouble(get(FALSE_POSITIVE_RATE, FALSE_POSITIVE_RATE_DEFAULT));
    }

    /**
     * Sets the allowable rate of false positives for bloom filters (Generally
     * the higher the value the faster the filter).
     *
     * @param falsePositiveRate the allowable rate of false positives for bloom
     *                          filters (Generally the higher the value the faster the filter).
     */
    public void setFalsePositiveRate(final String falsePositiveRate) {
        set(FALSE_POSITIVE_RATE, falsePositiveRate);
    }

    /**
     * Gets the size that should be used for the creation of bloom filters on the
     * server side.
     *
     * @return An integer representing the size that should be used for the
     * creation of bloom filters on the server side.
     */
    public int getMaxBloomFilterToPassToAnIterator() {
        return Integer.parseInt(
                get(MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR, MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR_DEFAULT));
    }

    /**
     * Sets the size that should be used for the creation of bloom filters on the
     * server side.
     *
     * @param maxBloomFilterToPassToAnIterator the size that should be used
     *                                         for the creation of bloom filters on the server side.
     */
    public void setMaxBloomFilterToPassToAnIterator(final String maxBloomFilterToPassToAnIterator) {
        set(MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR, maxBloomFilterToPassToAnIterator);
    }

    /**
     * Gets the key package that should be used in conjunction with this table.
     *
     * @return An implementation of
     * {@link uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage} to be used
     * for this accumulo table.
     */
    public String getKeyPackageClass() {
        return get(KEY_PACKAGE_CLASS, ByteEntityKeyPackage.class.getName());
    }

    /**
     * Sets the key package that should be used in conjunction with this table.
     *
     * @param keyPackageClass the key package that should be used in conjunction with this table.
     */
    public void setKeyPackageClass(final String keyPackageClass) {
        set(KEY_PACKAGE_CLASS, keyPackageClass);
    }

    /**
     * Gets the replication factor to be applied to tables created by Gaffer, if
     * not set then the table will use your general Accumulo settings default
     * value.
     *
     * @return The replication factor to be applied to tables created by Gaffer.
     */
    public String getTableFileReplicationFactor() {
        return get(TABLE_REPLICATION_FACTOR, null);
    }

    /**
     * Sets the replication factor to be applied to tables created by Gaffer, if
     * not set then the table will use your general Accumulo settings default
     * value.
     *
     * @param replicationFactor the replication factor to be applied to tables
     *                          created by gaffer, if not set then the table
     *                          will use your general accumulo settings default value.
     */
    public void setTableFileReplicationFactor(final String replicationFactor) {
        set(TABLE_REPLICATION_FACTOR, replicationFactor);
    }

    /**
     * Gets the flag determining whether the validator iterator should be enabled.
     *
     * @return true if the validator iterator should be enabled.
     */
    public boolean getEnableValidatorIterator() {
        return Boolean.parseBoolean(get(ENABLE_VALIDATOR_ITERATOR, ENABLE_VALIDATOR_ITERATOR_DEFAULT));
    }

    /**
     * Sets the flag determining whether the validator iterator should be enabled.
     *
     * @param enableValidatorIterator true if the validator iterator should be enabled.
     */
    public void setEnableValidatorIterator(final boolean enableValidatorIterator) {
        set(ENABLE_VALIDATOR_ITERATOR, Boolean.toString(enableValidatorIterator));
    }

    @Override
    public String getJsonSerialiserModules() {
        return new StringDeduplicateConcat().apply(
                SketchesJsonModules.class.getName(),
                super.getJsonSerialiserModules()
        );
    }
}
