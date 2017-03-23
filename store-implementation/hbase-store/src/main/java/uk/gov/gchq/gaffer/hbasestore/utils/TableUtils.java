/*
 * Copyright 2016-2017 Crown Copyright
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

import org.apache.commons.io.IOUtils;
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
import uk.gov.gchq.gaffer.commonutil.StringEscapeUtil;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.GafferCoprocessor;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        } catch (final Exception e) {
            // The method to create a table is synchronised, if you are using the same store only through one client in one JVM you shouldn't get here
            // Someone else got there first, never mind...
            throw new RuntimeException(e);
        }
    }

    public static Table getTable(final HBaseStore store) throws StoreException {
        final TableName tableName = store.getProperties().getTable();
        final Connection connection = store.getConnection();
        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            IOUtils.closeQuietly(connection);
            throw new StoreException(e);
        }
    }

    public static synchronized void createTable(final HBaseStore store)
            throws StoreException {
        final TableName tableName = store.getProperties().getTable();
        try {
            final Admin admin = store.getConnection().getAdmin();
            if (admin.tableExists(tableName)) {
                LOGGER.info("Table {} exists, not creating", tableName);
                return;
            }
            LOGGER.info("Creating table {}", tableName);

            final HTableDescriptor htable = new HTableDescriptor(tableName);
            final HColumnDescriptor col = new HColumnDescriptor(HBaseStoreConstants.getColFam());

            // TODO: disable versions
            col.setMaxVersions(Integer.MAX_VALUE);
            // Remove versioning iterator from table for all scopes
//            LOGGER.info("Removing versioning iterator from table {}", tableName);
//            final EnumSet<IteratorScope> iteratorScopes = EnumSet.allOf(IteratorScope.class);
//            connection.tableOperations().removeIterator(tableName, "vers", iteratorScopes);

            htable.addFamily(col);

            final String schemaJson = StringEscapeUtil.escapeComma(
                    Bytes.toString(store.getSchema().toCompactJson()));
            final Map<String, String> options = new HashMap<>(1);
            options.put(HBaseStoreConstants.SCHEMA, schemaJson);

            htable.addCoprocessor(GafferCoprocessor.class.getName(), store.getProperties().getDependencyJarsHdfsDirPath(), Coprocessor.PRIORITY_USER, options);
            admin.createTable(htable);
        } catch (Throwable e) {
            throw new StoreException(e.getMessage(), e);
        }

        ensureTableExists(store);
        LOGGER.info("Table {} created", tableName);
    }

    public static void deleteAllRows(final HBaseStore store, final String... auths) throws StoreException {
        final Connection connection = store.getConnection();
        try {
            if (connection.getAdmin().tableExists(store.getProperties().getTable())) {
                connection.getAdmin().flush(store.getProperties().getTable());
                final Table table = connection.getTable(store.getProperties().getTable());
                final Scan scan = new Scan();
                scan.setAuthorizations(new Authorizations(auths));
                try (ResultScanner scanner = table.getScanner(scan)) {
                    final List<Delete> deletes = new ArrayList<>();
                    for (final Result result : scanner) {
                        deletes.add(new Delete(result.getRow()));
                    }
                    table.delete(deletes);
                    connection.getAdmin().flush(store.getProperties().getTable());
                }

                try (ResultScanner scanner = table.getScanner(scan)) {
                    if (scanner.iterator().hasNext()) {
                        throw new StoreException("Some rows have not been deleted");
                    }
                }
            }
        } catch (final IOException e) {
            throw new StoreException(e);
        }
    }

    public static void dropTable(final HBaseStore store) throws StoreException {
        dropTable(store.getConnection(), store.getProperties());
    }

    public static void dropTable(final Connection connection, final HBaseProperties properties) throws StoreException {
        dropTable(connection, properties.getTable());
    }

    public static void dropTable(final Connection connection, final TableName tableName) throws StoreException {
        try {
            final Admin admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                LOGGER.info("Dropping table: " + tableName.getNameAsString());
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.deleteTable(tableName);
            }
        } catch (final IOException e) {
            throw new StoreException(e);
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
}
