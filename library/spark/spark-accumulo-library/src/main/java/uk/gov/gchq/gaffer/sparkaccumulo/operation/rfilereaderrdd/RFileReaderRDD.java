/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag$;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Creates an {@link RDD} of {@link Map.Entry}s of {@link Key}s and {@link Value}s for the data in the given table.
 */
public class RFileReaderRDD extends RDD<Map.Entry<Key, Value>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RFileReaderRDD.class);
    private static final String LAST_TABLET = "~~last~~";
    private final String instanceName;
    private final String zookeepers;
    private final String user;
    private final String password;
    private final String tableName;
    private final Set<String> auths;
    private final byte[] serialisedConfiguration;

    public RFileReaderRDD(final SparkContext sparkContext,
                          final String instanceName,
                          final String zookeepers,
                          final String user,
                          final String password,
                          final String tableName,
                          final Set<String> auths,
                          final byte[] serialisedConfiguration) {
        super(sparkContext, JavaConversions.asScalaBuffer(new ArrayList<>()),
                ClassTag$.MODULE$.apply(Map.Entry.class));
        this.instanceName = instanceName;
        this.zookeepers = zookeepers;
        this.user = user;
        this.password = password;
        this.tableName = tableName;
        this.auths = auths;
        this.serialisedConfiguration = serialisedConfiguration;
    }

    @Override
    public scala.collection.Iterator<Map.Entry<Key, Value>> compute(final Partition split, final TaskContext context) {
        final ByteArrayInputStream bais = new ByteArrayInputStream(serialisedConfiguration);
        final Configuration configuration = new Configuration();
        try {
            configuration.readFields(new DataInputStream(bais));
            bais.close();
        } catch (final IOException e) {
            throw new RuntimeException("IOException deserialising Configuration from byte array", e);
        }
        return new InterruptibleIterator<>(context,
                JavaConversions.asScalaIterator(new RFileReaderIterator(split, context, configuration, auths)));
    }

    @Override
    public Partition[] getPartitions() {
        // Get connection
        final Connector connector;
        try {
            final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
            connector = instance.getConnector(user, new PasswordToken(password));
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Exception connecting to Accumulo", e);
        }
        LOGGER.info("Obtained connection to instance {} as user {}", instanceName, user);

        try {
            // Check user has access
            if (!checkAccess(connector, user, tableName)) {
                throw new RuntimeException("User " + user + " does not have access to table" + tableName);
            }
            LOGGER.info("Confirmed user {} has access to table {}", user, tableName);

            // Get id and split points for table
            final String tableId = connector.tableOperations().tableIdMap().get(tableName);
            final int numTablets = connector.tableOperations().listSplits(tableName).size() + 1;
            LOGGER.info("Table {} has id {} and {} tablets", tableName, tableId, numTablets);

            // Create map from tablet name to information about that tablet, including location of the RFiles
            final Map<String, AccumuloTablet> tabletNameToInfo = createTabletMap(connector, user, tableId);

            // Create array of partitions
            final Partition[] partitions = new Partition[tabletNameToInfo.size()];
            for (final AccumuloTablet accumuloTablet : tabletNameToInfo.values()) {
                partitions[accumuloTablet.index()] = accumuloTablet;
            }
            LOGGER.info("Returning {} partitions", partitions.length);
            return partitions;
        } catch (final TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
            throw new RuntimeException("Exception creating partitions", e);
        }
    }

    private boolean checkAccess(final Connector connector, final String user, final String table) {
        try {
            if (!connector.securityOperations().hasTablePermission(user, table, TablePermission.READ)
                    && !connector.securityOperations().hasNamespacePermission(user, table, NamespacePermission.READ)) {
                return false;
            }
        } catch (final AccumuloException | AccumuloSecurityException e) {
            return false;
        }
        return true;
    }

    private Map<String, AccumuloTablet> createTabletMap(final Connector connector, final String user,
                                                        final String tableId)
            throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        // The table accumulo.metadata has the following form (information taken from
        // {@link https://accumulo.apache.org/1.8/accumulo_user_manual.html#metadata}).
        // - Every tablet has its own row
        // - Every row starts with the table id followed by ';' or '<' and ends with the end row split point for that tablet
        // - If the CF is 'file' then the CQ is the name of an RFile and the value contains the size of the file in bytes
        // and the number of key-value pairs in that file, separated by a ','

        // Create scanner (not batchscanner as we need the results to be in order) to read the rows of the
        // accumulo.metadata table relevant to table with id 'tableId'. As we only need file information, set the
        // CF to 'file'.
        LOGGER.info("Scanning accumulo.metadata table");
        final Authorizations auths = connector.securityOperations().getUserAuthorizations(user);
        final Scanner scanner = connector.createScanner("accumulo.metadata", auths);
        scanner.setRange(new Range(new Text(tableId), true, new Text(tableId + "<"), true));
        scanner.fetchColumnFamily(new Text("file"));

        final Map<String, AccumuloTablet> tabletMap = new HashMap<>();
        final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        int idx = 0;
        String lastTablet = null;
        String currentStart = null;
        if (!iterator.hasNext()) {
            LOGGER.warn("No Rfiles found");
        }
        while (iterator.hasNext()) {
            final Map.Entry<Key, Value> entry = iterator.next();
            final Key key = entry.getKey();

            // Row id is tableId;splitPoint
            // Last tablet is tableId<
            final String[] fields = key.getRow().toString().split(";");
            final String tabletName;
            if (fields.length == 2) {
                tabletName = fields[0];
            } else if (fields.length == 1) {
                tabletName = LAST_TABLET;
            } else {
                throw new RuntimeException("Row in accumulo.metadata didn't have the expected number of fields: "
                + "Expected 1 or 2, got " + fields.length);
            }

            // Detect start of a new tablet
            if (!tabletName.equals(lastTablet)) {
                currentStart = lastTablet;
                lastTablet = tabletName;
            }

            final String currentEnd;
            if (tabletName.equals(LAST_TABLET)) {
                currentEnd = null;
            } else {
                currentEnd = tabletName;
            }

            if (!tabletMap.containsKey(tabletName)) {
                tabletMap.put(tabletName, new AccumuloTablet(super.id(), idx, currentStart, currentEnd));
                idx += 1;
            }
            final AccumuloTablet tablet = tabletMap.get(tabletName);

            final String rFile = key.getColumnQualifier().toString();
            tablet.addRFile(rFile);
            LOGGER.info("Tablet {} has rFile {}", tabletName, rFile);
        }

        return tabletMap;
    }
}
