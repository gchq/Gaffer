/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.spark.SparkConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd.RFileReaderRDD;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RFileReaderRDDTest {
    private final String USER = "user";
    private final String PASSWORD = "password";
    private final String TABLE = "table";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void testRFileReaderRDDCanBeCreatedAndIsNonEmpty() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException, StoreException {
        // Given
        //  - Create MiniAccumuloCluster
        final MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempFolder.newFolder(), PASSWORD);
        final MiniAccumuloCluster cluster = new MiniAccumuloCluster(miniAccumuloConfig);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    cluster.stop();
                } catch (final IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        cluster.start();
        //  - Create user USER with permissions to create a table
        cluster.getConnector("root", PASSWORD).securityOperations().createLocalUser(USER, new PasswordToken(PASSWORD));
        cluster.getConnector("root", PASSWORD).securityOperations().grantSystemPermission(USER, SystemPermission.CREATE_TABLE);
        //  - Create AccumuloProperties
        final AccumuloProperties properties = new AccumuloProperties();
        properties.setStoreClass(AccumuloStore.class);
        properties.setInstance(cluster.getInstanceName());
        properties.setZookeepers(cluster.getZooKeepers());
        properties.setTable(TABLE);
        properties.setUser(USER);
        properties.setPassword(PASSWORD);
        //  - Add some data
        final Connector connector = cluster.getConnector(USER, PASSWORD);
        connector.tableOperations().create(TABLE);
        final BatchWriter bw = connector.createBatchWriter(TABLE, new BatchWriterConfig());
        final Mutation m1 = new Mutation("row");
        m1.put("CF", "CQ", "value");
        bw.addMutation(m1);
        final Mutation m2 = new Mutation("row2");
        m2.put("CF", "CQ", "not");
        bw.addMutation(m2);
        bw.close();
        //  - Compact to ensure an RFile is created, sleep to give it a little time to do it
        connector.tableOperations().compact(TABLE, new CompactionConfig());
        Thread.sleep(1000L);

        // When
        final SparkConf sparkConf = getSparkConf("testRFileReaderRDDCanBeCreatedAndIsNonEmpty");
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkConf,
                cluster.getInstanceName(), cluster.getZooKeepers(), USER, PASSWORD, TABLE,
                new HashSet<>(Arrays.asList("CF")), null, null);
        final long count = rdd.count();

        // Then
        assertEquals(2L, count);
        cluster.stop();
    }

    @Test
    public void testRFileReaderRDDAppliesIteratorCorrectly() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException, StoreException {
        // Given
        //  - Create MiniAccumuloCluster
        final MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempFolder.newFolder(), PASSWORD);
        final MiniAccumuloCluster cluster = new MiniAccumuloCluster(miniAccumuloConfig);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    cluster.stop();
                } catch (final IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        cluster.start();
        //  - Create user USER with permissions to create a table
        cluster.getConnector("root", PASSWORD).securityOperations().createLocalUser(USER, new PasswordToken(PASSWORD));
        cluster.getConnector("root", PASSWORD).securityOperations().grantSystemPermission(USER, SystemPermission.CREATE_TABLE);
        //  - Create AccumuloProperties
        final AccumuloProperties properties = new AccumuloProperties();
        properties.setStoreClass(AccumuloStore.class);
        properties.setInstance(cluster.getInstanceName());
        properties.setZookeepers(cluster.getZooKeepers());
        properties.setTable(TABLE);
        properties.setUser(USER);
        properties.setPassword(PASSWORD);
        //  - Add some data
        final Connector connector = cluster.getConnector(USER, PASSWORD);
        connector.tableOperations().create(TABLE);
        final BatchWriter bw = connector.createBatchWriter(TABLE, new BatchWriterConfig());
        final Mutation m1 = new Mutation("row");
        m1.put("CF", "CQ", "value");
        bw.addMutation(m1);
        final Mutation m2 = new Mutation("row2");
        m2.put("CF", "CQ", "not");
        bw.addMutation(m2);
        bw.close();
        //  - Compact to ensure an RFile is created, sleep to give it a little time to do it
        connector.tableOperations().compact(TABLE, new CompactionConfig());
        Thread.sleep(1000L);
        //  - Create an iterator and an option to grep for "val"
        final String iteratorClass = GrepIterator.class.getName();
        final Map<String, String> options = new HashMap<>();
        options.put("term", "val");

        // When
        final SparkConf sparkConf = getSparkConf("testRFileReaderRDDAppliesIteratorCorrectly");
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkConf,
                cluster.getInstanceName(), cluster.getZooKeepers(), USER, PASSWORD, TABLE,
                new HashSet<>(Arrays.asList("CF")), iteratorClass, options);
        final long count = rdd.count();

        // Then
        assertEquals(1L, count);
        cluster.stop();
    }

    private SparkConf getSparkConf(final String name) {
        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName(name)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        return sparkConf;
    }
}
