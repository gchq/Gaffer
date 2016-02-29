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

package gaffer.accumulostore;

import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.store.StoreProperties;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * AccumuloProperties contains specific configuration information for the
 * accumulo store, such as database connection strings. It wraps
 * {@link gaffer.data.element.Properties} and lazy loads the all properties from
 * a file when first used.
 */
public class AccumuloProperties extends StoreProperties {

    public static final String KEY_PACKAGE_CLASS = "gaffer.store.accumulo.keypackage.class";
    public static final String INSTANCE_NAME = "accumulo.instance";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    public static final String TABLE = "accumulo.table";
    public static final String USER = "accumulo.user";
    public static final String PASSWORD = "accumulo.password";
    public static final String AGE_OFF_TIME_IN_DAYS = "accumulo.ageOffTimeInDays";
    public static final String THREADS_FOR_BATCH_SCANNER = "accumulo.batchScannerThreads";
    public static final String MAX_ENTRIES_FOR_BATCH_SCANNER = "accumulo.entriesForBatchScanner";
    public static final String CLIENT_SIDE_BLOOM_FILTER_SIZE = "accumulo.clientSideBloomFilterSize";
    public static final String FALSE_POSITIVE_RATE = "accumulo.falsePositiveRate";
    public static final String MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR = "accumulo.maxBloomFilterToPassToAnIterator";
    public static final String MAX_BUFFER_SIZE_FOR_BATCH_WRITER = "accumulo.maxBufferSizeForBatchWriterInBytes";
    public static final String MAX_TIME_OUT_FOR_BATCH_WRITER = "accumulo.maxTimeOutForBatchWriterInMilliseconds";
    public static final String NUM_THREADS_FOR_BATCH_WRITER = "accumulo.numThreadsForBatchWriter";
    public static final String SPLITS_FILE_PATH = "accumulo.splits.file.path";
    public static final String TABLE_REPLICATION_FACTOR = "accumulo.file.replication";

    // default values
    private static final String NUM_THREADS_FOR_BATCH_WRITER_DEFAULT = "10";
    private static final String MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT = "50000";
    private static final String CLIENT_SIDE_BLOOM_FILTER_SIZE_DEFAULT = "838860800";
    private static final String FALSE_POSITIVE_RATE_DEFAULT = "0.0002";
    private static final String MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR_DEFAULT = "8388608";
    private static final String AGE_OFF_TIME_IN_DAYS_DEFAULT = "365";
    private static final String MAX_BUFFER_SIZE_FOR_BATCH_WRITER_DEFAULT = "1000000";
    private static final String MAX_TIME_OUT_FOR_BATCH_WRITER_DEFAULT = "1000";
    private static final String THREADS_FOR_BATCH_SCANNER_DEFAULT = "10";
    private static final String SPLITS_FILE_PATH_DEFAULT = "/data/splits.txt";

    public AccumuloProperties() {
        super();
    }

    public AccumuloProperties(final Path propFileLocation) {
        super(propFileLocation);
    }

    public static AccumuloProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return ((AccumuloProperties) StoreProperties.loadStoreProperties(storePropertiesStream));
    }

    public void setNumThreadsForBatchWriter(final String numThreadsForBatchWriter) {
        set(NUM_THREADS_FOR_BATCH_WRITER, numThreadsForBatchWriter);
    }

    public void setMaxTimeOutForBatchWriterInMilliseconds(final String maxTimeOutForBatchWriterInMilliseconds) {
        set(NUM_THREADS_FOR_BATCH_WRITER, MAX_TIME_OUT_FOR_BATCH_WRITER);
    }

    public void setMaxBufferSizeForBatchWriterInBytes(final String maxBufferSizeForBatchWriterInBytes) {
        set(MAX_BUFFER_SIZE_FOR_BATCH_WRITER, maxBufferSizeForBatchWriterInBytes);
    }

    /**
     * Gets the number of threads that should be used for the accumulo batch
     * writers
     *
     * @return The number of concurrent threads to use in the batch writer
     */
    public int getNumThreadsForBatchWriter() {
        return Integer.parseInt(get(NUM_THREADS_FOR_BATCH_WRITER, NUM_THREADS_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the time out/latency that should be used for the accumulo batch
     * writers
     *
     * @return The timeout to use on the batch writer
     */
    public Long getMaxTimeOutForBatchWriterInMilliseconds() {
        return Long.parseLong(get(MAX_TIME_OUT_FOR_BATCH_WRITER, MAX_TIME_OUT_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Gets the memory buffer size that should be used for the accumulo batch
     * writers
     *
     * @return The buffer size in bytes to use in the batch writer
     */
    public Long getMaxBufferSizeForBatchWriterInBytes() {
        return Long.parseLong(get(MAX_BUFFER_SIZE_FOR_BATCH_WRITER, MAX_BUFFER_SIZE_FOR_BATCH_WRITER_DEFAULT));
    }

    /**
     * Get the list of Zookeeper servers.
     *
     * @return A comma separated list of Zookeeper servers
     */
    public String getZookeepers() {
        return get(ZOOKEEPERS);
    }

    /**
     * Set the list of Zookeeper servers.
     *
     * @param zookeepers the list of Zookeeper servers
     */
    public void setZookeepers(final String zookeepers) {
        set(ZOOKEEPERS, zookeepers);
    }

    /**
     * Get the Accumulo instance name.
     *
     * @return Return the instance name of accumulo set in the properties file
     */
    public String getInstanceName() {
        return get(INSTANCE_NAME);
    }

    /**
     * Set the Accumulo instance name.
     *
     * @param instanceName the Accumulo instance name
     */
    public void setInstanceName(final String instanceName) {
        set(INSTANCE_NAME, instanceName);
    }

    /**
     * Get the particular table name.
     *
     * @return The accumulo table to use as set in the properties file
     */
    public String getTable() {
        return get(TABLE);
    }

    /**
     * Set the table name.
     *
     * @param tableName the table name
     */
    public void setTable(final String tableName) {
        set(TABLE, tableName);
    }

    /**
     * Get the configured Accumulo user.
     *
     * @return Get the configured accumulo user
     */
    public String getUserName() {
        return get(USER);
    }

    /**
     * Set the configured Accumulo user.
     *
     * @param userName the configured Accumulo user
     */
    public void setUserName(final String userName) {
        set(USER, userName);
    }

    /**
     * Get the password for the Accumulo user.
     *
     * @return the password for the configured accumulo user
     */
    public String getPassword() {
        return get(PASSWORD);
    }

    /**
     * Set the password to use for the Accumulo user.
     *
     * @param password the password to use for the Accumulo user
     */
    public void setPassword(final String password) {
        set(PASSWORD, password);
    }

    /**
     * Get the number of days data should be retained
     *
     * @return AN integer representing the number of days data should be
     * retained
     */
    public int getAgeOffTimeInDays() {
        return Integer.parseInt(get(AGE_OFF_TIME_IN_DAYS, AGE_OFF_TIME_IN_DAYS_DEFAULT));
    }

    /**
     * Set the number of days data should be retained
     *
     * @param ageOffTimeInDays the number of days data should be retained
     */
    public void setAgeOffTimeInDays(final String ageOffTimeInDays) {
        set(AGE_OFF_TIME_IN_DAYS, ageOffTimeInDays);
    }

    /**
     * Get the number of threads to use in the batch scanner
     *
     * @return An integer representing the number of threads to use in the batch
     * scanner
     */
    public int getThreadsForBatchScanner() {
        return Integer.parseInt(get(THREADS_FOR_BATCH_SCANNER, THREADS_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Set the number of threads to use in the batch scanner
     *
     * @param threadsForBatchScanner the number of threads to use in the batch scanner
     */
    public void setThreadsForBatchScanner(final String threadsForBatchScanner) {
        set(THREADS_FOR_BATCH_SCANNER, threadsForBatchScanner);
    }

    /**
     * Get the max number of items that should be read into the scanner at any
     * one time
     *
     * @return An integer representing the max number of items that should be
     * read into the scanner at any one time
     */
    public int getMaxEntriesForBatchScanner() {
        return Integer.parseInt(get(MAX_ENTRIES_FOR_BATCH_SCANNER, MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Set the max number of items that should be read into the scanner at any
     * one time
     *
     * @param maxEntriesForBatchScanner the max number of items that should be read into the scanner at any one time
     */
    public void setMaxEntriesForBatchScanner(final String maxEntriesForBatchScanner) {
        set(MAX_ENTRIES_FOR_BATCH_SCANNER, maxEntriesForBatchScanner);
    }

    /**
     * Get the size that should be used for the creation of bloom filters on the
     * client side
     *
     * @return An integer representing the size that should be used for the
     * creation of bloom filters on the client side
     */
    public int getClientSideBloomFilterSize() {
        return Integer.parseInt(get(CLIENT_SIDE_BLOOM_FILTER_SIZE, CLIENT_SIDE_BLOOM_FILTER_SIZE_DEFAULT));
    }

    /**
     * Set the size that should be used for the creation of bloom filters on the
     * client side
     *
     * @param clientSideBloomFilterSize the size that should be used for the creation of bloom filters on the client side
     */
    public void setClientSideBloomFilterSize(final String clientSideBloomFilterSize) {
        set(CLIENT_SIDE_BLOOM_FILTER_SIZE, clientSideBloomFilterSize);
    }

    /**
     * Get the allowable rate of false positives for bloom filters (Generally
     * the higher the value the faster the filter)
     *
     * @return A number representing the rate of false positives for bloom
     * filters (Generally the higher the value the faster the filter)
     */
    public double getFalsePositiveRate() {
        return Double.parseDouble(get(FALSE_POSITIVE_RATE, FALSE_POSITIVE_RATE_DEFAULT));
    }

    /**
     * Set the allowable rate of false positives for bloom filters (Generally
     * the higher the value the faster the filter)
     *
     * @param falsePositiveRate the allowable rate of false positives for bloom filters (Generally the higher the value the faster the filter)
     */
    public void setFalsePositiveRate(final String falsePositiveRate) {
        set(FALSE_POSITIVE_RATE, falsePositiveRate);
    }

    /**
     * Get the size that should be used for the creation of bloom filters on the
     * server side
     *
     * @return An integer representing the size that should be used for the
     * creation of bloom filters on the server side
     */
    public int getMaxBloomFilterToPassToAnIterator() {
        return Integer.parseInt(
                get(MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR, MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR_DEFAULT));
    }

    /**
     * Set the size that should be used for the creation of bloom filters on the
     * server side
     *
     * @param maxBloomFilterToPassToAnIterator the size that should be used for the creation of bloom filters on the server side
     */
    public void setMaxBloomFilterToPassToAnIterator(final String maxBloomFilterToPassToAnIterator) {
        set(MAX_BLOOM_FILTER_TO_PASS_TO_AN_ITERATOR, maxBloomFilterToPassToAnIterator);
    }

    /**
     * Get the key package that should be used in conjunction with this table
     *
     * @return An implementation of
     * {@link gaffer.accumulostore.key.AccumuloKeyPackage} to be used
     * for this accumulo table
     */
    public String getKeyPackageClass() {
        return get(KEY_PACKAGE_CLASS, ByteEntityKeyPackage.class.getName());
    }

    /**
     * Set the key package that should be used in conjunction with this table
     *
     * @param keyPackageClass the key package that should be used in conjunction with this table
     */
    public void setKeyPackageClass(final String keyPackageClass) {
        set(KEY_PACKAGE_CLASS, keyPackageClass);
    }

    /**
     * Get the path of a splits file to be automatically when using the
     * accumulo-stores built in partitioner strategy
     *
     * @return the path of a splits file to be automatically when using the accumulo-stores built in partitioner strategy
     */
    public String getSplitsFilePath() {
        return get(SPLITS_FILE_PATH, SPLITS_FILE_PATH_DEFAULT);
    }

    /**
     * Set the path of a splits file to be automatically when using the
     * accumulo-stores built in partitioner strategy
     *
     * @param splitsFilePath the path of a splits file to be automatically when using the accumulo-stores built in partitioner strategy
     */
    public void setSplitsFilePath(final String splitsFilePath) {
        set(SPLITS_FILE_PATH, splitsFilePath);
    }

    /**
     * Get the replication factor to be applied to tables created by gaffer, if
     * not set then the table will use your general accumulo settings default
     * value.
     *
     * @return The replication factor to be applied to tables created by gaffer
     */
    public String getTableFileReplicationFactor() {
        return get(TABLE_REPLICATION_FACTOR, null);
    }

    /**
     * Set the replication factor to be applied to tables created by gaffer, if
     * not set then the table will use your general accumulo settings default
     * value.
     *
     * @param replicationFactor the replication factor to be applied to tables created by gaffer, if not set then the table will use your general accumulo settings default value.
     */
    public void setTableFileReplicationFactor(final String replicationFactor) {
        set(TABLE_REPLICATION_FACTOR, replicationFactor);
    }
}
