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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.MiniAccumuloClusterProvider;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd.RFileReaderRDD;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RFileReaderRddIT {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void testRFileReaderRDDCanBeCreatedAndIsNonEmpty() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException, StoreException {
        // Given
        final String table = "table1";
        final MiniAccumuloCluster cluster = MiniAccumuloClusterProvider.getMiniAccumuloCluster();
        // Add some data
        final Connector connector = cluster.getConnector(MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD);
        connector.tableOperations().create(table);
        final BatchWriter bw = connector.createBatchWriter(table, new BatchWriterConfig());
        final Mutation m1 = new Mutation("row");
        m1.put("CF", "CQ", "value");
        bw.addMutation(m1);
        final Mutation m2 = new Mutation("row2");
        m2.put("CF", "CQ", "not");
        bw.addMutation(m2);
        bw.close();
        // Compact to ensure an RFile is created, sleep to give it a little time to do it
        connector.tableOperations().compact(table, new CompactionConfig());
        Thread.sleep(1000L);

        // When
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        final Configuration conf = new Configuration();
        InputConfigurator.fetchColumns(AccumuloInputFormat.class, conf,
                Sets.newHashSet(new Pair<>(new Text("CF"), new Text("CQ"))));
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD, table, new HashSet<>(),
                serialiseConfiguration(conf));
        final long count = rdd.count();

        // Then
        assertEquals(2L, count);
    }

    @Test
    public void testRFileReaderRDDAppliesIteratorCorrectly() throws IOException,
            InterruptedException, AccumuloSecurityException, AccumuloException, TableNotFoundException,
            TableExistsException, StoreException {
        // Given
        final String table = "table2";
        final MiniAccumuloCluster cluster = MiniAccumuloClusterProvider.getMiniAccumuloCluster();
        // Add some data
        final Connector connector = cluster.getConnector(MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD);
        connector.tableOperations().create(table);
        final BatchWriter bw = connector.createBatchWriter(table, new BatchWriterConfig());
        final Mutation m1 = new Mutation("row");
        m1.put("CF", "CQ", "value");
        bw.addMutation(m1);
        final Mutation m2 = new Mutation("row2");
        m2.put("CF", "CQ", "not");
        bw.addMutation(m2);
        bw.close();
        // Compact to ensure an RFile is created, sleep to give it a little time to do it
        connector.tableOperations().compact(table, new CompactionConfig());
        Thread.sleep(1000L);
        // Create an iterator and an option to grep for "val"
        final Map<String, String> options = new HashMap<>();
        options.put("term", "val");
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf);
        AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "NAME", GrepIterator.class.getName(), options));
        InputConfigurator.fetchColumns(AccumuloInputFormat.class, job.getConfiguration(),
                Sets.newHashSet(new Pair<>(new Text("CF"), new Text("CQ"))));

        // When
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        final RFileReaderRDD rdd = new RFileReaderRDD(sparkSession.sparkContext(),
                cluster.getInstanceName(), cluster.getZooKeepers(), MiniAccumuloClusterProvider.USER,
                MiniAccumuloClusterProvider.PASSWORD, table, new HashSet<>(),
                serialiseConfiguration(job.getConfiguration()));
        final long count = rdd.count();

        // Then
        assertEquals(1L, count);
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
