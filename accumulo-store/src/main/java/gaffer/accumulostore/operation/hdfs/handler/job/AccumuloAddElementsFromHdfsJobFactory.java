/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.accumulostore.operation.hdfs.handler.job;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.IngestUtils;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.operation.simple.hdfs.handler.AbstractAddElementsFromHdfsJobFactory;
import gaffer.store.Store;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AccumuloAddElementsFromHdfsJobFactory extends AbstractAddElementsFromHdfsJobFactory {
    public static final String ELEMENT_CONVERTER = "elementConverter";

    @Override
    protected void setupJobConf(final JobConf jobConf, final AddElementsFromHdfs operation, final Store store) throws IOException {
        super.setupJobConf(jobConf, operation, store);
        jobConf.set(ELEMENT_CONVERTER, ((AccumuloStore) store).getKeyPackage().getKeyConverter().getClass().getName());
    }

    @Override
    public void setupJob(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        super.setupJob(job, operation, store);

        setupMapper(job, operation, store);
        setupReducer(job, operation, store);
        setupOutput(job, operation, store);
        setupSplits(job, operation, (AccumuloStore) store);
    }

    private void setupMapper(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        job.setMapperClass(AddElementsFromHdfsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
    }

    private void setupReducer(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        job.setReducerClass(AddElementsFromHdfsReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
    }

    private void setupOutput(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        AccumuloFileOutputFormat.setOutputPath(job, operation.getOutputPath());
    }

    private void setupSplits(final Job job, final AddElementsFromHdfs operation, final AccumuloStore store) throws IOException {
        final String splitsFilePath = store.getProperties().getSplitsFilePath();
        final int numReduceTasks;
        try {
            numReduceTasks = IngestUtils.createSplitsFile(store.getConnection(), store.getProperties().getTable(),
                    FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
        } catch (StoreException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        job.setNumReduceTasks(numReduceTasks + 1);
        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }
}
