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

package uk.gov.gchq.gaffer.hbasestore;

import org.apache.hadoop.hbase.TableName;

import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * HBaseProperties contains specific configuration information for the
 * hbase store, such as database connection strings. It wraps
 * {@link uk.gov.gchq.gaffer.data.element.Properties} and lazy loads the all
 * properties from
 * a file when first used.
 */
public class HBaseProperties extends StoreProperties {
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

    public HBaseProperties() {
        super(HBaseStore.class);
    }

    public HBaseProperties(final Path propFileLocation) {
        super(propFileLocation, HBaseStore.class);
    }

    public static HBaseProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, HBaseProperties.class);
    }

    public static HBaseProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, HBaseProperties.class);
    }

    public static HBaseProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, HBaseProperties.class);
    }

    @Override
    public HBaseProperties clone() {
        return (HBaseProperties) super.clone();
    }

    public org.apache.hadoop.fs.Path getDependencyJarsHdfsDirPath() {
        final String path = get(DEPENDENCY_JARS_HDFS_DIR_PATH);
        return null != path ? new org.apache.hadoop.fs.Path(path) : null;
    }

    public void setDependencyJarsHdfsDirPath(final String path) {
        set(DEPENDENCY_JARS_HDFS_DIR_PATH, path);
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
     * @return The hbase table name
     * @deprecated use {@link HBaseStore#getTableName}
     */
    @Deprecated
    public String getTableName() {
        return get(TABLE);
    }

    /**
     * Get the particular table.
     *
     * @return The hbase table
     * @deprecated use {@link HBaseStore#getTable}
     */
    @Deprecated
    public TableName getTable() {
        return TableName.valueOf(getTableName());
    }

    /**
     * Set the table name.
     *
     * @param table the table name
     * @deprecated use a graphId
     */
    @Deprecated
    public void setTable(final String table) {
        set(TABLE, table);
    }

    public int getWriteBufferSize() {
        final String bufferSize = get(WRITE_BUFFER_SIZE, null);
        if (null == bufferSize) {
            return WRITE_BUFFER_SIZE_DEFAULT;
        }

        return Integer.parseInt(bufferSize);
    }

    public void setWriteBufferSize(final int size) {
        set(WRITE_BUFFER_SIZE, String.valueOf(size));
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
     * @param maxEntriesForBatchScanner the max number of items that should be
     *                                  read into the scanner at any one time
     */
    public void setMaxEntriesForBatchScanner(final String maxEntriesForBatchScanner) {
        set(MAX_ENTRIES_FOR_BATCH_SCANNER, maxEntriesForBatchScanner);
    }

    @Override
    public String getJsonSerialiserModules() {
        return new StringDeduplicateConcat().apply(
                SketchesJsonModules.class.getName(),
                super.getJsonSerialiserModules()
        );
    }
}
