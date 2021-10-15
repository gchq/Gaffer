/*
 * Copyright 2017-2021 Crown Copyright
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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.MiniAccumuloClusterProvider;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd.RFileReaderRDD;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class RFileReaderRddIT {

    private final Configuration config = new Configuration();
    private final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
    private static int nextTableId;
    private static String tableName;

    @BeforeEach
    public void setUp() {
        nextTableId++;
        tableName = "table" + nextTableId;
    }

    @Test
    public void testRFileReaderRDDCanBeCreatedWith2TableInputs() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException {
        // Given
        final List<String> dataInput = Arrays.asList("apples", "bananas");
        final MiniAccumuloCluster cluster = createAccumuloCluster(tableName, config, dataInput);

        // When
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD, tableName, new HashSet<>(),
                serialiseConfiguration(config));

        // Then
        assertThat(rdd.count()).isEqualTo(dataInput.size());
        assertThat(rdd.getPartitions()).hasSize(1);
    }

    @Test
    public void testRFileReaderRDDCanBeCreatedWith5TableInputs() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException {
        // Given
        final List<String> dataInput = Arrays.asList("train", "plane", "automobile", "bike", "boat");
        final MiniAccumuloCluster cluster = createAccumuloCluster(tableName, config, dataInput);

        // When
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD, tableName, new HashSet<>(),
                serialiseConfiguration(config));

        // Then
        assertThat(rdd.count()).isEqualTo(dataInput.size());
        assertThat(rdd.getPartitions()).hasSize(1);
    }

    @Test
    public void testRFileReaderRDDAppliesIteratorCorrectly() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException {
        // Given
        final List<String> data = Arrays.asList("no", "not", "value");
        final Job job = Job.getInstance(config);
        final MiniAccumuloCluster cluster = createAccumuloCluster(tableName, job.getConfiguration(), data);

        // Create an iterator and an option to grep for "val"
        final Map<String, String> options = new HashMap<>();
        options.put("term", "val");
        AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "NAME", GrepIterator.class.getName(), options));

        // When
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD, tableName, new HashSet<>(),
                serialiseConfiguration(job.getConfiguration()));

        // Then
        assertThat(rdd.count()).isEqualTo(1L);
    }

    public void throwRTX_whenGetPartitionsForFileReaderWithInvalidTableName() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException {
        // Given
        final MiniAccumuloCluster cluster = createAccumuloCluster(tableName, config, Arrays.asList("Bananas"));
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD, "Invalid Table Name", new HashSet<>(),
                serialiseConfiguration(config));

        // When / Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(rdd::getPartitions)
                .withMessage("User user does not have access to table Invalid Table Name");
    }

    @Test
    public void throwRTX_whenRDDHasUserWithoutPermission() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException {
        // Given
        final MiniAccumuloCluster cluster = createAccumuloCluster(tableName, config, Arrays.asList("Bananas"));
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER_NO_GRANTED_PERMISSION,
                MiniAccumuloClusterProvider.PASSWORD, tableName, new HashSet<>(),
                serialiseConfiguration(config));

        // When / Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(rdd::getPartitions)
                .withMessage("User user2 does not have access to table " + tableName);
    }

    @Test
    public void throwRTX_whenRDDHasIncorrectUser() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException {
        // Given
        final MiniAccumuloCluster cluster = createAccumuloCluster(tableName, config, Arrays.asList("Bananas"));
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), "Incorrect Username", "", tableName,
                new HashSet<>(), serialiseConfiguration(config));

        // When / Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(rdd::getPartitions)
                .withMessage("Exception connecting to Accumulo");
    }

    private MiniAccumuloCluster createAccumuloCluster(final String tableName, final Configuration configuration, final List<String> data)
            throws InterruptedException, AccumuloException, AccumuloSecurityException, IOException,
            TableExistsException, TableNotFoundException {

        final MiniAccumuloCluster cluster = MiniAccumuloClusterProvider.getMiniAccumuloCluster();
        final Connector connector = cluster.getConnector(MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD);
        connector.tableOperations().create(tableName);

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

        return cluster;
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
