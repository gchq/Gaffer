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

package uk.gov.gchq.gaffer.hbasestore;

import org.apache.hadoop.hbase.TableName;

import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StorePropertiesUtil;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

public final class HBaseStorePropertiesUtil {
    public static final String ZOOKEEPERS = "hbase.zookeepers";
    /**
     * @deprecated use a graphId
     */
    @Deprecated
    public static final String TABLE = "hbase.table";
    public static final String WRITE_BUFFER_SIZE = "hbase.writeBufferSize";
    public static final String DEPENDENCY_JARS_HDFS_DIR_PATH = "hbase.hdfs.jars.path";
    public static final String MAX_ENTRIES_FOR_BATCH_SCANNER = "hbase.entriesForBatchScanner";
    public static final int WRITE_BUFFER_SIZE_DEFAULT = 1000000;
    public static final String MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT = "50000";

    private HBaseStorePropertiesUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    public static org.apache.hadoop.fs.Path getDependencyJarsHdfsDirPath(final StoreProperties hBaseProperties) {
        final String path = hBaseProperties.getProperty(DEPENDENCY_JARS_HDFS_DIR_PATH);
        return null != path ? new org.apache.hadoop.fs.Path(path) : null;
    }

    public static void setDependencyJarsHdfsDirPath(final StoreProperties hBaseProperties, final String path) {
        hBaseProperties.setProperty(DEPENDENCY_JARS_HDFS_DIR_PATH, path);
    }

    /**
     * Get the list of Zookeeper servers.
     *
     * @param hBaseProperties the properties instance
     * @return A comma separated list of Zookeeper servers
     */
    public static String getZookeepers(final StoreProperties hBaseProperties) {
        return hBaseProperties.getProperty(ZOOKEEPERS);
    }

    /**
     * Set the list of Zookeeper servers.
     *
     * @param hBaseProperties the properties instance
     * @param zookeepers      the list of Zookeeper servers
     */
    public static void setZookeepers(final StoreProperties hBaseProperties, final String zookeepers) {
        hBaseProperties.setProperty(ZOOKEEPERS, zookeepers);
    }

    /**
     * @param hBaseProperties the properties instance
     * @return The hbase table name
     * @deprecated use {@link HBaseStore#getTableName}
     */
    @Deprecated
    public static String getTableName(final StoreProperties hBaseProperties) {
        return hBaseProperties.getProperty(TABLE);
    }

    /**
     * Get the particular table.
     *
     * @param hBaseProperties the properties instance
     * @return The hbase table
     * @deprecated use {@link HBaseStore#getTable}
     */
    @Deprecated
    public static TableName getTable(final StoreProperties hBaseProperties) {
        return TableName.valueOf(getTableName(hBaseProperties));
    }

    /**
     * Set the table name.
     *
     * @param hBaseProperties the properties instance
     * @param table           the table name
     * @deprecated use a graphId
     */
    @Deprecated
    public static void setTable(final StoreProperties hBaseProperties, final String table) {
        hBaseProperties.setProperty(TABLE, table);
    }

    public static int getWriteBufferSize(final StoreProperties hBaseProperties) {
        final String bufferSize = hBaseProperties.getProperty(WRITE_BUFFER_SIZE, null);
        if (null == bufferSize) {
            return WRITE_BUFFER_SIZE_DEFAULT;
        }

        return Integer.parseInt(bufferSize);
    }

    public static void setWriteBufferSize(final StoreProperties hBaseProperties, final int size) {
        hBaseProperties.setProperty(WRITE_BUFFER_SIZE, String.valueOf(size));
    }

    /**
     * Get the max number of items that should be read into the scanner at any
     * one time
     *
     * @param hBaseProperties the properties instance
     * @return An integer representing the max number of items that should be
     * read into the scanner at any one time
     */
    public static int getMaxEntriesForBatchScanner(final StoreProperties hBaseProperties) {
        return Integer.parseInt(hBaseProperties.getProperty(MAX_ENTRIES_FOR_BATCH_SCANNER, MAX_ENTRIES_FOR_BATCH_SCANNER_DEFAULT));
    }

    /**
     * Set the max number of items that should be read into the scanner at any
     * one time
     *
     * @param hBaseProperties           the properties instance
     * @param maxEntriesForBatchScanner the max number of items that should be
     *                                  read into the scanner at any one time
     */
    public static void setMaxEntriesForBatchScanner(final StoreProperties hBaseProperties, final String maxEntriesForBatchScanner) {
        hBaseProperties.setProperty(MAX_ENTRIES_FOR_BATCH_SCANNER, maxEntriesForBatchScanner);
    }

    public static String getJsonSerialiserModules(final StoreProperties hBaseProperties) {
        return new StringDeduplicateConcat().apply(
                SketchesJsonModules.class.getName(),
                StorePropertiesUtil.getJsonSerialiserModules(hBaseProperties)
        );
    }
}
