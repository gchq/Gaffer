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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.partitioner.GafferKeyRangePartitioner;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer.AccumuloKeyValueReducer;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IngestUtils;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.hdfs.operation.partitioner.NoPartitioner;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;

public class AccumuloAddElementsFromHdfsJobFactory implements AddElementsFromHdfsJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloAddElementsFromHdfsJobFactory.class);
    public static final String INVALID_FIELD_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ONE_GOT_S = "Invalid field - must be >=1, got %s";
    public static final String INGEST_HDFS_DATA_GENERATOR_S_OUTPUT_S = "Ingest HDFS data: Generator = %s, output = %s";

    @Override
    public void prepareStore(final Store store) throws StoreException {
        TableUtils.ensureTableExists(((AccumuloStore) store));
    }

    @Override
    public JobConf createJobConf(final AddElementsFromHdfs operation, final String mapperGeneratorClassName, final Store store) throws IOException {
        final JobConf jobConf = new JobConf(new Configuration());

        LOGGER.info("Setting up job conf");
        jobConf.set(SCHEMA, new String(store.getSchema().toCompactJson(), CommonConstants.UTF_8));
        LOGGER.debug("Added {} {} to job conf", SCHEMA, new String(store.getSchema().toCompactJson(), CommonConstants.UTF_8));
        jobConf.set(MAPPER_GENERATOR, mapperGeneratorClassName);
        LOGGER.info("Added {} of {} to job conf", MAPPER_GENERATOR, mapperGeneratorClassName);
        jobConf.set(VALIDATE, String.valueOf(operation.isValidate()));
        LOGGER.info("Added {} option of {} to job conf", VALIDATE, operation.isValidate());

        if (null != operation.getNumMapTasks()) {
            jobConf.setNumMapTasks(operation.getNumMapTasks());
            LOGGER.info("Set number of map tasks to {} on job conf", operation.getNumMapTasks());
        }

        if (null != operation.getNumReduceTasks()) {
            jobConf.setNumReduceTasks(operation.getNumReduceTasks());
            LOGGER.info("Set number of reduce tasks to {} on job conf", operation.getNumReduceTasks());
        }

        jobConf.set(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ((AccumuloStore) store).getKeyPackage().getKeyConverter().getClass().getName());

        return jobConf;
    }

    protected String getJobName(final String mapperGenerator, final String outputPath) {
        return String.format(INGEST_HDFS_DATA_GENERATOR_S_OUTPUT_S, mapperGenerator, outputPath);
    }

    @Override
    public void setupJob(final Job job, final AddElementsFromHdfs operation, final String mapperGenerator, final Store store) throws IOException {
        job.setJarByClass(getClass());
        job.setJobName(getJobName(mapperGenerator, operation.getOutputPath()));

        setupMapper(job);
        setupCombiner(job);
        setupReducer(job);
        setupOutput(job, operation);

        if (!NoPartitioner.class.equals(operation.getPartitioner())) {
            if (null != operation.getPartitioner()) {
                operation.setPartitioner(GafferKeyRangePartitioner.class);
                LOGGER.warn("Partitioner class " + operation.getPartitioner().getName() + " will be replaced with " + GafferKeyRangePartitioner.class.getName());
            }
            setupPartitioner(job, operation, (AccumuloStore) store);
        }
    }

    protected void setupMapper(final Job job) {
        job.setMapperClass(AddElementsFromHdfsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
    }

    protected void setupCombiner(final Job job) {
        job.setCombinerClass(AccumuloKeyValueReducer.class);
    }

    protected void setupReducer(final Job job) {
        job.setReducerClass(AccumuloKeyValueReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
    }

    protected void setupOutput(final Job job, final AddElementsFromHdfs operation) {
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(operation.getOutputPath()));
    }

    protected void setupPartitioner(final Job job, final AddElementsFromHdfs operation, final AccumuloStore store) throws IOException {
        if (operation.isUseProvidedSplits()) {
            // Use provided splits file
            setUpPartitionerFromUserProvidedSplitsFile(job, operation);
        } else {
            // User didn't provide a splits file
            setUpPartitionerGenerateSplitsFile(job, operation, store);
        }
    }

    protected void setUpPartitionerGenerateSplitsFile(final Job job, final AddElementsFromHdfs operation,
                                                      final AccumuloStore store) throws IOException {
        final String splitsFilePath = operation.getSplitsFilePath();
        LOGGER.info("Creating splits file in location {} from table {}", splitsFilePath, store.getTableName());

        final int minReducers;
        final int maxReducers;
        int numReducers;
        if (validateValue(operation.getNumReduceTasks()) != 0) {
            minReducers = validateValue(operation.getNumReduceTasks());
            maxReducers = validateValue(operation.getNumReduceTasks());
        } else {
            minReducers = validateValue(operation.getMinReduceTasks());
            maxReducers = validateValue(operation.getMaxReduceTasks());
        }

        try {
            numReducers = 1 + IngestUtils.createSplitsFile(store.getConnection(), store.getTableName(),
                    FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
            if (maxReducers != 0 && maxReducers < numReducers) {
                // maxReducers set and Accumulo given more reducers than we want
                numReducers = 1 + IngestUtils.createSplitsFile(store.getConnection(), store.getTableName(),
                        FileSystem.get(job.getConfiguration()), new Path(splitsFilePath), maxReducers - 1);
            }
        } catch (final StoreException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        if (minReducers != 0 && numReducers < minReducers) {
            // minReducers set and Accumulo given less reducers than we want, set the appropriate number of subbins
            LOGGER.info("Number of reducers is {} which is less than the specified minimum number of {}", numReducers,
                    minReducers);
            int factor = (minReducers / numReducers) + 1;
            LOGGER.info("Setting number of subbins on GafferKeyRangePartitioner to {}", factor);
            GafferKeyRangePartitioner.setNumSubBins(job, factor);
            numReducers = numReducers * factor;
            LOGGER.info("Number of reducers is {}", numReducers);
        }

        if (maxReducers != 0 && numReducers > maxReducers) {
            throw new IllegalArgumentException(minReducers + " - " + maxReducers + " is not a valid range, consider increasing the maximum reducers to at least " + numReducers);
        }

        job.setNumReduceTasks(numReducers);
        job.setPartitionerClass(GafferKeyRangePartitioner.class);
        GafferKeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }

    protected void setUpPartitionerFromUserProvidedSplitsFile(final Job job, final AddElementsFromHdfs operation)
            throws IOException {
        final String splitsFilePath = operation.getSplitsFilePath();
        if (validateValue(operation.getMaxReduceTasks()) != -1
                || validateValue(operation.getMinReduceTasks()) != -1) {
            LOGGER.info("Using splits file provided by user {}, ignoring minReduceTasks and maxReduceTasks", splitsFilePath);
        } else {
            LOGGER.info("Using splits file provided by user {}", splitsFilePath);
        }
        final int numSplits = IngestUtils.getNumSplits(FileSystem.get(job.getConfiguration()), new Path(splitsFilePath));
        job.setNumReduceTasks(numSplits + 1);
        job.setPartitionerClass(GafferKeyRangePartitioner.class);
        GafferKeyRangePartitioner.setSplitFile(job, splitsFilePath);
    }

    protected static int validateValue(final Integer value) throws IOException {
        int result = 0;
        if (null != value) {
            result = value;
            if (result < 1) {
                LOGGER.error(String.format(INVALID_FIELD_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ONE_GOT_S, result));
                throw new IllegalArgumentException(String.format(INVALID_FIELD_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ONE_GOT_S, value));
            }
        }
        return result;
    }
}
