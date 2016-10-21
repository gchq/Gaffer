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

package uk.gov.gchq.gaffer.accumulostore.inputformat;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InputFormatTest {

    private enum KeyPackage {
        BYTE_ENTITY_KEY_PACKAGE,
        CLASSIC_KEY_PACKAGE
    }

    private static final int NUM_ENTRIES = 1000;
    private static final List<Element> DATA = new ArrayList<>();
    static {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("" + i);
            entity.putProperty("property1", 1);

            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("" + i);
            edge.setDestination("B");
            edge.setDirected(true);
            edge.putProperty("property1", 2);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(true);
            edge2.putProperty("property2", 3);

            DATA.add(edge);
            DATA.add(edge2);
            DATA.add(entity);
        }
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void shouldReturnCorrectDataToMapReduceJob() throws Exception {
        final View view = new View.Builder().build();
        final Set<String> expectedResults = new HashSet<>();
        for (final Element element : DATA) {
            expectedResults.add(element.toString());
        }
        shouldReturnCorrectDataToMapReduceJob(KeyPackage.BYTE_ENTITY_KEY_PACKAGE,
                view,
                "instance1",
                expectedResults);
        shouldReturnCorrectDataToMapReduceJob(KeyPackage.CLASSIC_KEY_PACKAGE,
                view,
                "instance2",
                expectedResults);
    }

    @Test
    public void shouldReturnCorrectDataToMapReduceJobWithView() throws Exception {
        final View view = new View.Builder().edge(TestGroups.EDGE).build();
        final Set<String> expectedResults = new HashSet<>();
        for (final Element element : DATA) {
            if (element.getGroup().equals(TestGroups.EDGE)) {
                expectedResults.add(element.toString());
            }
        }
        shouldReturnCorrectDataToMapReduceJob(KeyPackage.BYTE_ENTITY_KEY_PACKAGE,
                view,
                "instance3",
                expectedResults);
        shouldReturnCorrectDataToMapReduceJob(KeyPackage.CLASSIC_KEY_PACKAGE,
                view,
                "instance4",
                expectedResults);
    }

    private void shouldReturnCorrectDataToMapReduceJob(final KeyPackage kp, final View view,
                                                       final String instanceName, final Set<String> expectedResults)
            throws Exception {
        final AccumuloStore store = new MockAccumuloStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        switch (kp) {
            case BYTE_ENTITY_KEY_PACKAGE:
                properties.setKeyPackageClass(ByteEntityKeyPackage.class.getName());
                properties.setInstanceName(instanceName + "_BYTE_ENTITY");
                break;
            case CLASSIC_KEY_PACKAGE:
                properties.setKeyPackageClass(ClassicKeyPackage.class.getName());
                properties.setInstanceName(instanceName + "_CLASSIC");
        }
        try {
            store.initialise(schema, properties);
        } catch (StoreException e) {
            fail("StoreException thrown: " + e);
        }
        setupGraph(store);

        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        final FileSystem fs = FileSystem.getLocal(conf);

        // Update configuration with instance, table name, etc.
        store.updateConfiguration(conf, view);

        // Run Driver
        final File outputFolder = testFolder.newFolder();
        FileUtils.deleteDirectory(outputFolder);
        final Driver driver = new Driver(outputFolder.getAbsolutePath());
        driver.setConf(conf);
        driver.run(new String[]{});

        // Read results and check correct
        final SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputFolder + "/part-m-00000"), conf);
        final Text text = new Text();
        final Set<String> results = new HashSet<>();
        while (reader.next(text)) {
            results.add(text.toString());
        }
        reader.close();
        assertEquals(expectedResults, results);
        FileUtils.deleteDirectory(outputFolder);
    }

    private void setupGraph(final AccumuloStore store) {
        try {
            store.execute(new AddElements(DATA), new User());
        } catch (OperationException e) {
            fail("Couldn't add elements: " + e);
        }
    }

    private class Driver extends Configured implements Tool {

        private final String outputDir;

        Driver(final String outputDir) {
            this.outputDir = outputDir;
        }

        @Override
        public int run(final String[] args) throws Exception {
            final Configuration conf = getConf();
            final Job job = new Job(conf);
            job.setJarByClass(getClass());
            job.setInputFormatClass(ElementInputFormat.class);
            job.setMapperClass(AMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
            job.setNumReduceTasks(0);
            job.waitForCompletion(true);

            return job.isSuccessful() ? 0 : 1;
        }
    }

    private static class AMapper extends Mapper<Element, NullWritable, Text, NullWritable> {

        protected void map(final Element key, final NullWritable nw, final Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), nw);
        }
    }

}
