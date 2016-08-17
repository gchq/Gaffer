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

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class AccumuloAddElementsFromHdfsJobFactory extends AbstractAddElementsFromHdfsJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloAddElementsFromHdfsJobFactory.class);

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

        setupMapper(job);
        setupCombiner(job);
        setupReducer(job);
        setupOutput(job, operation);

        final String useAccumuloPartitioner = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_USE_ACCUMULO_PARTITIONER);
        if (null == useAccumuloPartitioner || useAccumuloPartitioner.equalsIgnoreCase("true")) {
            setupPartitioner(job, operation, (AccumuloStore) store);
        }
    }

    private void setupMapper(final Job job) throws IOException {
        job.setMapperClass(AddElementsFromHdfsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
    }

    private void setupCombiner(final Job job) throws IOException {
        job.setCombinerClass(AccumuloKeyValueReducer.class);
    }

    private void setupReducer(final Job job) throws IOException {
        job.setReducerClass(AccumuloKeyValueReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
    }

    private void setupOutput(final Job job, final AddElementsFromHdfs operation) throws IOException {
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(operation.getOutputPath()));
    }

    private void setupPartitioner(final Job job, final AddElementsFromHdfs operation, final AccumuloStore store)
            throws IOException {
        String splitsFilePath = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE);
        final String maxReducersString = operation.getOption(AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS);
        int numSplits;
        if (null == splitsFilePath || splitsFilePath.equals("")) {
            // User didn't provide a splits file
            splitsFilePath = store.getProperties().getSplitsFilePath();
            LOGGER.info("Creating splits file in location {}", splitsFilePath);
            int maxReducers = -1;
            if (maxReducersString != null && !maxReducersString.equals("")) {
                try {
                    maxReducers = Integer.parseInt(maxReducersString);
                } catch (final NumberFormatException e) {
                    LOGGER.error("Error parsing maximum number of reducers option, got {}", maxReducersString);
                    throw new RuntimeException("Can't parse " + AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE
                            + " option, got " + maxReducersString);
                }
                if (maxReducers < 1) {
                    LOGGER.error("Invalid maximum number of reducers option - must be >=1, got {}", maxReducers);
                    throw new RuntimeException(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE + " must be >= 1");
                }
                LOGGER.info("Maximum number of reducers option is {}", maxReducers);
            }
            try {
                if (maxReducers == -1) {
                    numSplits = IngestUtils.createSplitsFile(store.getConnection(), store.getProperties().getTable(),
                            FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
                } else {
                    numSplits = IngestUtils.createSplitsFile(store.getConnection(), store.getProperties().getTable(),
                            FileSystem.get(job.getConfiguration()), new Path(splitsFilePath), maxReducers - 1);
                }
            } catch (final StoreException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            numSplits = IngestUtils.getNumSplits(FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
            if (maxReducersString != null && !maxReducersString.equals("")) {
                LOGGER.info("Found {} splits in user provided splits file {}, ignoring {} option", numSplits,
                        splitsFilePath,
                        AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS);
            } else {
                LOGGER.info("Found {} splits in user provided splits file {}", numSplits, splitsFilePath);
            }
        }
        job.setNumReduceTasks(numSplits + 1);
        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }
}
