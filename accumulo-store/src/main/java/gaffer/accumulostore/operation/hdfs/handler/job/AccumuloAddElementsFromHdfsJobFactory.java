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
package gaffer.accumulostore.operation.hdfs.handler.job;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.IngestUtils;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.operation.simple.hdfs.handler.AbstractAddElementsFromHdfsJobFactory;
import gaffer.store.Store;
import gaffer.store.StoreException;

public class AccumuloAddElementsFromHdfsJobFactory extends AbstractAddElementsFromHdfsJobFactory {

    @Override
    protected void setupJobConf(final JobConf jobConf, final AddElementsFromHdfs operation, final Store store)
            throws IOException {
        super.setupJobConf(jobConf, operation, store);
        jobConf.set(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ((AccumuloStore) store).getKeyPackage().getKeyConverter().getClass().getName());
    }

    @Override
    public void setupJob(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        super.setupJob(job, operation, store);

        setupMapper(job, operation, store);
        setupReducer(job, operation, store);
        setupOutput(job, operation, store);
        String useAccumuloPartioner = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_USE_ACCUMULO_PARTITIONER);
        if (null != useAccumuloPartioner && useAccumuloPartioner.equalsIgnoreCase("true")) {
            setupPartioner(job, operation, (AccumuloStore) store);
        }
    }

    private void setupMapper(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        job.setMapperClass(AddElementsFromHdfsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
    }

    private void setupReducer(final Job job, final AddElementsFromHdfs operation, final Store store)
            throws IOException {
        job.setReducerClass(AddElementsFromHdfsReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
    }

    private void setupOutput(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, operation.getOutputPath());
    }

    private void setupPartioner(final Job job, final AddElementsFromHdfs operation, final AccumuloStore store)
            throws IOException {
        String splitsFilePath = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE);
        int numReduceTasks;
        if (null == splitsFilePath || splitsFilePath.equals("")) {
            splitsFilePath = store.getProperties().getSplitsFilePath();
            try {
                numReduceTasks = IngestUtils.createSplitsFile(store.getConnection(), store.getProperties().getTable(),
                        FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
            } catch (final StoreException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            numReduceTasks = IngestUtils.getNumSplits(FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
        }
        job.setNumReduceTasks(numReduceTasks + 1);
        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }
}
