/*
 * Copyright 2017-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.integration.rfilereaderrdd;

import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd.RFileReaderRDD;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RFileReaderRddIT {

    private final Configuration config = new Configuration();
    private final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
    private static int nextTableId;
    private static String tableName;

    private static MiniAccumuloClusterManager miniAccumuloClusterManager;
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(RFileReaderRddIT.class.getResourceAsStream("/store.properties"));

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @ClassRule
    public static TemporaryFolder storeBaseFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setUpStore() {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, storeBaseFolder.getRoot().getAbsolutePath());
    }

    @AfterClass
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @Before
    public void setUp() {
        nextTableId++;
        tableName = "table" + nextTableId;
    }

    @Test
    public void testRFileReaderRDDCanBeCreatedWith2TableInputs() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException,
            StoreException, TableNotFoundException {

        // Given
        final List<String> dataInput = Arrays.asList("apples", "bananas");
        loadAccumuloCluster(tableName, config, dataInput);

        // When
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                PROPERTIES.getInstance(), PROPERTIES.getZookeepers(), PROPERTIES.getUser(),
                PROPERTIES.getPassword(), tableName, new HashSet<>(),
                serialiseConfiguration(config));

        // Then
        assertEquals(dataInput.size(), rdd.count());
        assertEquals(1, rdd.getPartitions().length);
    }

    @Test
    public void testRFileReaderRDDCanBeCreatedWith5TableInputs() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException,
            StoreException, TableNotFoundException {

        // Given
        final List<String> dataInput = Arrays.asList("train", "plane", "automobile", "bike", "boat");
        loadAccumuloCluster(tableName, config, dataInput);

        // When
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                PROPERTIES.getInstance(), PROPERTIES.getZookeepers(), PROPERTIES.getUser(),
                PROPERTIES.getPassword(), tableName, new HashSet<>(),
                serialiseConfiguration(config));

        // Then
        assertEquals(dataInput.size(), rdd.count());
        assertEquals(1, rdd.getPartitions().length);
    }

    @Test
    public void testRFileReaderRDDAppliesIteratorCorrectly() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException,
            StoreException, TableNotFoundException {
        // Given
        final List<String> data = Arrays.asList("no", "not", "value");
        final Job job = Job.getInstance(config);
        loadAccumuloCluster(tableName, job.getConfiguration(), data);

        // Create an iterator and an option to grep for "val"
        final Map<String, String> options = new HashMap<>();
        options.put("term", "val");
        AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "NAME", GrepIterator.class.getName(), options));

        // When
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                PROPERTIES.getInstance(), PROPERTIES.getZookeepers(), PROPERTIES.getUser(),
                PROPERTIES.getPassword(), tableName, new HashSet<>(),
                serialiseConfiguration(job.getConfiguration()));

        // Then
        assertEquals(1L, rdd.count());
    }

    @Test
    public void throwRTX_whenGetPartitionsForFileReaderWithInvalidTableName() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException,
            StoreException, TableNotFoundException {

        // Given
        loadAccumuloCluster(tableName, config, Arrays.asList("Bananas"));
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                PROPERTIES.getInstance(), PROPERTIES.getZookeepers(), PROPERTIES.getUser(),
                PROPERTIES.getPassword(), "Invalid Table Name", new HashSet<>(),
                serialiseConfiguration(config));

        // When
        RuntimeException actual = assertThrows(RuntimeException.class, () -> rdd.getPartitions());

        // Then
        assertEquals("User " + PROPERTIES.getUser() + " does not have access to table Invalid Table Name",
                actual.getMessage());
    }

    @Test
    public void throwRTX_whenRDDHasUserWithoutPermission() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException,
            StoreException, TableNotFoundException {
        // Given
        final String user = "user2";
        loadAccumuloCluster(tableName, config, Arrays.asList("Bananas"));
        miniAccumuloClusterManager.getCluster().getConnector("root", miniAccumuloClusterManager.ROOTPW).securityOperations()
                .createLocalUser(user, new PasswordToken(PROPERTIES.getPassword()));

        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                PROPERTIES.getInstance(), PROPERTIES.getZookeepers(), "user2",
                PROPERTIES.getPassword(), tableName, new HashSet<>(),
                serialiseConfiguration(config));

        // When
        RuntimeException actual = assertThrows(RuntimeException.class, () -> rdd.getPartitions());

        // Then
        assertEquals("User " + user + " does not have access to table " + tableName,
                actual.getMessage());
    }

    @Test
    public void throwRTX_whenRDDHasIncorrectUser() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException,
            StoreException, TableNotFoundException {

        // Given
        loadAccumuloCluster(tableName, config, Arrays.asList("Bananas"));
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                PROPERTIES.getInstance(), PROPERTIES.getZookeepers(), "Incorrect Username",
                "", tableName, new HashSet<>(), serialiseConfiguration(config));

        // When
        RuntimeException actual = assertThrows(RuntimeException.class, () -> rdd.getPartitions());

        // Then
        assertEquals("Exception connecting to Accumulo",
                actual.getMessage());
    }

    private void loadAccumuloCluster(final String tableName, final Configuration configuration, final List<String> data)
            throws InterruptedException, AccumuloException, AccumuloSecurityException, StoreException, TableNotFoundException {

        AccumuloStore accumuloStore = new AccumuloStore();
        accumuloStore.initialise(tableName, new Schema(), PROPERTIES);
        final Connector connector = accumuloStore.getConnection();

        // Add data
        final BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
        for (int i = 0; i < data.size(); i++) {
            final Mutation m = new Mutation("row" + i);
            m.put("CF", "CQ", data.get(i));
            bw.addMutation(m);
        }
        bw.close();

        // Compact to ensure an RFile is created, sleep to give it a little time to do it
        connector.tableOperations().compact(tableName, new CompactionConfig());
        Thread.sleep(1000L);

        InputConfigurator.fetchColumns(AccumuloInputFormat.class, configuration,
                Sets.newHashSet(new Pair<>(new Text("CF"), new Text("CQ"))));
    }

    private byte[] serialiseConfiguration(final Configuration configuration) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        configuration.write(dos);
        final byte[] serialised = baos.toByteArray();
        baos.close();
        return serialised;
    }
}
