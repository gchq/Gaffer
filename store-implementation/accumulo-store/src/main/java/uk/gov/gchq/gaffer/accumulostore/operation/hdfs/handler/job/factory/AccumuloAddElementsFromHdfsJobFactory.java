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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory;

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
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer.AccumuloKeyValueReducer;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IngestUtils;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AbstractAddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;

public class AccumuloAddElementsFromHdfsJobFactory extends
        AbstractAddElementsFromHdfsJobFactory {
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

        boolean userSplitsFile = false;
        final String splitsFilePath = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE_PATH);

        final String userSplitsFileStr = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_USE_PROVIDED_SPLITS_FILE);

        if (userSplitsFileStr != null) {
            userSplitsFile = Boolean.parseBoolean(userSplitsFileStr);
        }

        if (splitsFilePath == null) {
            // Provide a default path if the splits file path is missing
            operation.addOption(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE_PATH, "");
            LOGGER.warn("HDFS splits file path not set - using the current directory as the default path.");
        }

        if (userSplitsFile) {
            // Use provided splits file
            setUpPartitionerFromUserProvidedSplitsFile(job, operation);
        } else {
            // User didn't provide a splits file
            setUpPartitionerGenerateSplitsFile(job, operation, store);
        }
    }

    private void setUpPartitionerGenerateSplitsFile(final Job job, final AddElementsFromHdfs operation,
                                                    final AccumuloStore store) throws IOException {
        final String splitsFilePath = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE_PATH);
        LOGGER.info("Creating splits file in location {} from table {}", splitsFilePath, store.getProperties().getTable());
        final int maxReducers = intOptionIsValid(operation, AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS);
        final int minReducers = intOptionIsValid(operation, AccumuloStoreConstants.OPERATION_BULK_IMPORT_MIN_REDUCERS);
        if (maxReducers != -1 && minReducers != -1) {
            if (minReducers > maxReducers) {
                LOGGER.error("Minimum number of reducers must be less than the maximum number of reducers: minimum was {} "
                        + "maximum was {}", minReducers, maxReducers);
                throw new IOException("Minimum number of reducers must be less than the maximum number of reducers");
            }
        }
        int numSplits;
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
        int numReducers = numSplits + 1;
        LOGGER.info("Number of splits is {}; number of reducers is {}", numSplits, numReducers);
        // If neither min or max are specified then nothing to do; if max specified and min not then already taken care of.
        // If min is specified and the number of reducers is not greater than that then set the appropriate number of
        // subbins.
        if (minReducers != -1) {
            if (numReducers < minReducers) {
                LOGGER.info("Number of reducers is {} which is less than the specified minimum number of {}", numReducers,
                        minReducers);
                int factor = (minReducers / numReducers) + 1;
                LOGGER.info("Setting number of subbins on KeyRangePartitioner to {}", factor);
                KeyRangePartitioner.setNumSubBins(job, factor);
                numReducers = numReducers * factor;
                LOGGER.info("Number of reducers is {}", numReducers);
            }
        }
        job.setNumReduceTasks(numReducers);
        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }

    private void setUpPartitionerFromUserProvidedSplitsFile(final Job job, final AddElementsFromHdfs operation)
            throws IOException {
        final String splitsFilePath = operation.getOption(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE_PATH);
        if (intOptionIsValid(operation, AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS) != -1
                || intOptionIsValid(operation, AccumuloStoreConstants.OPERATION_BULK_IMPORT_MIN_REDUCERS) != -1) {
            LOGGER.info("Using splits file provided by user {}, ignoring options {} and {}", splitsFilePath,
                    AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS,
                    AccumuloStoreConstants.OPERATION_BULK_IMPORT_MIN_REDUCERS);
        } else {
            LOGGER.info("Using splits file provided by user {}", splitsFilePath);
        }
        final int numSplits = IngestUtils.getNumSplits(FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
        job.setNumReduceTasks(numSplits + 1);
        job.setPartitionerClass(KeyRangePartitioner.class);
        KeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }

    private static int intOptionIsValid(final AddElementsFromHdfs operation, final String optionKey) throws IOException {
        final String option = operation.getOption(optionKey);
        int result = -1;
        if (option != null && !option.equals("")) {
            try {
                result = Integer.parseInt(option);
            } catch (final NumberFormatException e) {
                LOGGER.error("Error parsing {}, got {}", optionKey, option);
                throw new IOException("Can't parse " + optionKey + " option, got " + option, e);
            }
            if (result < 1) {
                LOGGER.error("Invalid {} option - must be >=1, got {}", optionKey, result);
                throw new IOException("Invalid " + optionKey + " option - must be >=1, got " + result);
            }
            LOGGER.info("{} option is {}", optionKey, result);
        }
        return result;
    }
}
