/*
 * Copyright 2019 Crown Copyright
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
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

public final class AccumuloStorePropertiesUtil {

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

    private AccumuloStorePropertiesUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    /**
     * Sets the number of threads that should be used for the Accumulo batch
     * writers.
     *
     * @param accumuloProperties       the StoreProperties to add to the StoreProperties to add to
     * @param numThreadsForBatchWriter The number of concurrent threads to use in the batch writer.
     */
    public static void setNumThreadsForBatchWriter(final StoreProperties accumuloProperties, final String numThreadsForBatchWriter) {
        accumuloProperties.setProperty(NUM_THREADS_FOR_BATCH_WRITER, numThreadsForBatchWriter);
    }

    /**
     * Sets the time out/latency that should be used for the Accumulo batch
     * writers.
     *
     * @param accumuloProperties                     the StoreProperties to add to the StoreProperties to add to
     * @param maxTimeOutForBatchWriterInMilliseconds The timeout to use on the batch writer.
     */
    public static void setMaxTimeOutForBatchWriterInMilliseconds(final StoreProperties accumuloProperties, final String maxTimeOutForBatchWriterInMilliseconds) {
        accumuloProperties.setProperty(MAX_TIME_OUT_FOR_BATCH_WRITER, maxTimeOutForBatchWriterInMilliseconds);
    }

    /**
     * Sets the memory buffer size that should be used for the Accumulo batch
     * writers.
     *
     * @param accumuloProperties                 the StoreProperties to add to the StoreProperties to add to
     * @param maxBufferSizeForBatchWriterInBytes The buffer size in bytes to use in the batch writer.
     */
    public static void setMaxBufferSizeForBatchWriterInBytes(final StoreProperties accumuloProperties, final String maxBufferSizeForBatchWriterInBytes) {
        accumuloProperties.setProperty(MAX_BUFFER_SIZE_FOR_BATCH_WRITER, maxBufferSizeForBatchWriterInBytes);
    }

    /**
     * Gets the number of threads that should be used for the Accumulo batch
     * writers.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return The number of concurrent threads to use in the batch writer.
     */
    public static int getNumThreadsForBatchWriter(final StoreProperties accumuloProperties) {
        return Integer.parseInt(accumuloProperties.getProperty(NUM_THREADS_FOR_BATCH_WRITER, NUM_THREADS_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the time out/latency that should be used for the Accumulo batch
     * writers.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return The timeout to use on the batch writer.
     */
    public static Long getMaxTimeOutForBatchWriterInMilliseconds(final StoreProperties accumuloProperties) {
        return Long.parseLong(accumuloProperties.getProperty(MAX_TIME_OUT_FOR_BATCH_WRITER, MAX_TIME_OUT_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the memory buffer size that should be used for the Accumulo batch
     * writers.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return The buffer size in bytes to use in the batch writer.
     */
    public static Long getMaxBufferSizeForBatchWriterInBytes(final StoreProperties accumuloProperties) {
        return Long.parseLong(accumuloProperties.getProperty(MAX_BUFFER_SIZE_FOR_BATCH_WRITER, MAX_BUFFER_SIZE_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the list of Zookeeper servers.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return A comma separated list of Zookeeper servers.
     */
    public static String getZookeepers(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(ZOOKEEPERS);
    }

    /**
     * Sets the list of Zookeeper servers.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @param zookeepers         the list of Zookeeper servers.
     */
    public static void setZookeepers(final StoreProperties accumuloProperties, final String zookeepers) {
        accumuloProperties.setProperty(ZOOKEEPERS, zookeepers);
    }

    /**
     * Gets the Accumulo instance name.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return Return the instance name of Accumulo set in the properties file.
     */
    public static String getInstance(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(INSTANCE_NAME);
    }

    /**
     * Sets the Accumulo instance name.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @param instance           the Accumulo instance name.
     */
    public static void setInstance(final StoreProperties accumuloProperties, final String instance) {
        accumuloProperties.setProperty(INSTANCE_NAME, instance);
    }

    /**
     * Gets the particular table name.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return The Accumulo table to use as set in the properties file.
     * @deprecated use {@link AccumuloStore#getTableName}.
     */
    @Deprecated
    public static String getTable(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(TABLE);
    }

    /**
     * Sets the table name.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @param tableName          the table name.
     * @deprecated use a graphId.
     */
    @Deprecated
    public static void setTable(final StoreProperties accumuloProperties, final String tableName) {
        accumuloProperties.setProperty(TABLE, tableName);
    }

    /**
     * Gets the configured Accumulo user.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return Get the configured accumulo user.
     */
    public static String getUser(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(USER);
    }

    /**
     * Sets the configured Accumulo user.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @param user               the configured Accumulo user.
     */
    public static void setUser(final StoreProperties accumuloProperties, final String user) {
        accumuloProperties.setProperty(USER, user);
    }

    /**
     * Gets the password for the Accumulo user.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return the password for the configured Accumulo user.
     */
    public static String getPassword(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(PASSWORD);
    }

    /**
     * Sets the password to use for the Accumulo user.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @param password           the password to use for the Accumulo user.
     */
    public static void setPassword(final StoreProperties accumuloProperties, final String password) {
        accumuloProperties.setProperty(PASSWORD, password);
    }

    /**
     * Gets the number of threads to use in the batch scanner.
     *
     * @param accumuloProperties the StoreProperties to add to the StoreProperties to add to
     * @return An integer representing the number of threads to use in the batch
     * scanner.
     */
    public static int getThreadsForBatchScanner(final StoreProperties accumuloProperties) {
        return Integer.parseInt(accumuloProperties.getProperty(THREADS_FOR_BATCH_SCANNER, THREADS_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Sets the number of threads to use in the batch scanner.
     *
     * @param accumuloProperties     the StoreProperties to add to the StoreProperties to add to
     * @param threadsForBatchScanner the number of threads to use in the batch scanner.
     */
    public static void setThreadsForBatchScanner(final StoreProperties accumuloProperties, final String threadsForBatchScanner) {
        accumuloProperties.setProperty(THREADS_FOR_BATCH_SCANNER, threadsForBatchScanner);
    }

    /**
     * Gets the max number of items that should be read into the scanner at any
     * one time.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return An integer representing the max number of items that should be
     * read into the scanner at any one time.
     */
    public static int getMaxEntriesForBatchScanner(final StoreProperties accumuloProperties) {
        return Integer.parseInt(accumuloProperties.getProperty(MAX_ENTRIES_FOR_BATCH_SCANNER, MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Sets the max number of items that should be read into the scanner at any
     * one time.
     *
     * @param accumuloProperties        the StoreProperties to add to
     * @param maxEntriesForBatchScanner the max number of items that should be read into the scanner at any one time.
     */
    public static void setMaxEntriesForBatchScanner(final StoreProperties accumuloProperties, final String maxEntriesForBatchScanner) {
        accumuloProperties.setProperty(MAX_ENTRIES_FOR_BATCH_SCANNER, maxEntriesForBatchScanner);
    }

    /**
     * Gets the size that should be used for the creation of bloom filters on the
     * client side.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return An integer representing the size that should be used for the
     * creation of bloom filters on the client side.
     */
    public static int getClientSideBloomFilterSize(final StoreProperties accumuloProperties) {
        return Integer.parseInt(accumuloProperties.getProperty(CLIENT_SIDE_BLOOM_FILTER_SIZE, CLIENT_SIDE_BLOOM_FILTER_SIZE_DEFAULT));
    }

    /**
     * Sets the size that should be used for the creation of bloom filters on the
     * client side.
     *
     * @param accumuloProperties        the StoreProperties to add to
     * @param clientSideBloomFilterSize the size that should be used for the creation of bloom filters on the client side.
     */
    public static void setClientSideBloomFilterSize(final StoreProperties accumuloProperties, final String clientSideBloomFilterSize) {
        accumuloProperties.setProperty(CLIENT_SIDE_BLOOM_FILTER_SIZE, clientSideBloomFilterSize);
    }

    /**
     * Gets the allowable rate of false positives for bloom filters (Generally
     * the higher the value the faster the filter).
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return A number representing the rate of false positives for bloom
     * filters (Generally the higher the value the faster the filter).
     */
    public static double getFalsePositiveRate(final StoreProperties accumuloProperties) {
        return Double.parseDouble(accumuloProperties.getProperty(FALSE_POSITIVE_RATE, FALSE_POSITIVE_RATE_DEFAULT));
    }

    /**
     * Sets the allowable rate of false positives for bloom filters (Generally
     * the higher the value the faster the filter).
     *
     * @param accumuloProperties the StoreProperties to add to
     * @param falsePositiveRate  the allowable rate of false positives for bloom
     *                           filters (Generally the higher the value the faster the filter).
     */
    public static void setFalsePositiveRate(final StoreProperties accumuloProperties, final String falsePositiveRate) {
        accumuloProperties.setProperty(FALSE_POSITIVE_RATE, falsePositiveRate);
    }

    /**
     * Gets the size that should be used for the creation of bloom filters on the
     * server side.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return An integer representing the size that should be used for the
     * creation of bloom filters on the server side.
     */
    public static int getMaxBloomFilterToPassToAnIterator(final StoreProperties accumuloProperties) {
        return Integer.parseInt(
                accumuloProperties.getProperty(MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR, MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR_DEFAULT));
    }

    /**
     * Sets the size that should be used for the creation of bloom filters on the
     * server side.
     *
     * @param accumuloProperties               the StoreProperties to add to
     * @param maxBloomFilterToPassToAnIterator the size that should be used
     *                                         for the creation of bloom filters on the server side.
     */
    public static void setMaxBloomFilterToPassToAnIterator(final StoreProperties accumuloProperties, final String maxBloomFilterToPassToAnIterator) {
        accumuloProperties.setProperty(MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR, maxBloomFilterToPassToAnIterator);
    }

    /**
     * Gets the key package that should be used in conjunction with this table.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return An implementation of
     * {@link uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage} to be used
     * for this accumulo table.
     */
    public static String getKeyPackageClass(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(KEY_PACKAGE_CLASS, ByteEntityKeyPackage.class.getName());
    }

    /**
     * Sets the key package that should be used in conjunction with this table.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @param keyPackageClass    the key package that should be used in conjunction with this table.
     */
    public static void setKeyPackageClass(final StoreProperties accumuloProperties, final String keyPackageClass) {
        accumuloProperties.setProperty(KEY_PACKAGE_CLASS, keyPackageClass);
    }

    /**
     * Gets the replication factor to be applied to tables created by Gaffer, if
     * not set then the table will use your general Accumulo settings default
     * value.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return The replication factor to be applied to tables created by Gaffer.
     */
    public static String getTableFileReplicationFactor(final StoreProperties accumuloProperties) {
        return accumuloProperties.getProperty(TABLE_REPLICATION_FACTOR, null);
    }

    /**
     * Sets the replication factor to be applied to tables created by Gaffer, if
     * not set then the table will use your general Accumulo settings default
     * value.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @param replicationFactor  the replication factor to be applied to tables
     *                           created by gaffer, if not set then the table
     *                           will use your general accumulo settings default value.
     */
    public static void setTableFileReplicationFactor(final StoreProperties accumuloProperties, final String replicationFactor) {
        accumuloProperties.setProperty(TABLE_REPLICATION_FACTOR, replicationFactor);
    }

    /**
     * Gets the flag determining whether the validator iterator should be enabled.
     *
     * @param accumuloProperties the StoreProperties to add to
     * @return true if the validator iterator should be enabled.
     */
    public static boolean getEnableValidatorIterator(final StoreProperties accumuloProperties) {
        return Boolean.parseBoolean(accumuloProperties.getProperty(ENABLE_VALIDATOR_ITERATOR, ENABLE_VALIDATOR_ITERATOR_DEFAULT));
    }

    /**
     * Sets the flag determining whether the validator iterator should be enabled.
     *
     * @param accumuloProperties      the StoreProperties to add to
     * @param enableValidatorIterator true if the validator iterator should be enabled.
     */
    public static void setEnableValidatorIterator(final StoreProperties accumuloProperties, final boolean enableValidatorIterator) {
        accumuloProperties.setProperty(ENABLE_VALIDATOR_ITERATOR, Boolean.toString(enableValidatorIterator));
    }

    public static String getJsonSerialiserModules(final StoreProperties storeProperties) {
        return new StringDeduplicateConcat().apply(
                SketchesJsonModules.class.getName(),
                StorePropertiesUtil.getJsonSerialiserModules(storeProperties)
        );
    }
}
