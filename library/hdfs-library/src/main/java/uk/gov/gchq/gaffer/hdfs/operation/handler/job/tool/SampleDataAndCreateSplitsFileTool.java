/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.hdfs.operation.handler.job.tool;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.SampleDataForSplitPointsJobFactory;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Store;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class SampleDataAndCreateSplitsFileTool extends Configured implements Tool {
    public static final int SUCCESS_RESPONSE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataAndCreateSplitsFileTool.class);

    private final SampleDataForSplitPoints operation;
    private final Store store;
    private final SampleDataForSplitPointsJobFactory jobFactory;
    private final int expectedNumberOfSplits;

    public SampleDataAndCreateSplitsFileTool(final SampleDataForSplitPointsJobFactory jobFactory, final SampleDataForSplitPoints operation, final Store store) {
        this.operation = operation;
        this.store = store;
        this.jobFactory = jobFactory;
        if (null == operation.getNumSplits() || operation.getNumSplits() < 1) {
            expectedNumberOfSplits = jobFactory.getExpectedNumberOfSplits(store);
        } else {
            expectedNumberOfSplits = operation.getNumSplits();
        }
    }

    @Override
    public int run(final String[] strings) throws OperationException {
        final List<Job> jobs;
        try {
            LOGGER.info("Creating job using SampleDataForSplitPointsJobFactory");
            jobs = jobFactory.createJobs(operation, store);
        } catch (final IOException e) {
            LOGGER.error("Failed to create Hadoop job: {}", e.getMessage());
            throw new OperationException("Failed to create the Hadoop job: " + e.getMessage(), e);
        }

        for (final Job job : jobs) {
            try {
                LOGGER.info("Running SampleDataForSplitPoints job (job name is {})", job.getJobName());
                job.waitForCompletion(true);
            } catch (final IOException | InterruptedException | ClassNotFoundException e) {
                LOGGER.error("Exception running job: {}", e.getMessage());
                throw new OperationException("Error while waiting for job to complete: " + e.getMessage(), e);
            }

            try {
                if (!job.isSuccessful()) {
                    LOGGER.error("Job was not successful (job name is {})", job.getJobName());
                    throw new OperationException("Error running job");
                }
            } catch (final IOException e) {
                LOGGER.error("Exception running job: {}", e.getMessage());
                throw new OperationException("Error running job" + e.getMessage(), e);
            }


            // Find the number of records output
            // NB In the following line use mapred.Task.Counter.REDUCE_OUTPUT_RECORDS rather than
            // mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS as this is more compatible with earlier
            // versions of Hadoop.
            Counter counter;
            try {
                counter = job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
                LOGGER.info("Number of records output = {}", counter.getValue());
            } catch (final IOException e) {
                LOGGER.error("Failed to get counter org.apache.hadoop.mapred.TaskCounter.REDUCE_OUTPUT_RECORDS from job: {}", e.getMessage());
                throw new OperationException("Failed to get counter: " + TaskCounter.REDUCE_OUTPUT_RECORDS, e);
            }


            long outputEveryNthRecord;
            if (counter.getValue() < 2 || expectedNumberOfSplits < 1) {
                outputEveryNthRecord = 1;
            } else {
                outputEveryNthRecord = counter.getValue() / expectedNumberOfSplits;
            }

            if (outputEveryNthRecord < 1) {
                outputEveryNthRecord = 1;
            }

            final Path resultsFile = new Path(operation.getOutputPath(), "part-r-00000");
            LOGGER.info("Will output every {}-th record from {}", outputEveryNthRecord, resultsFile);

            // Read through resulting file, pick out the split points and write to file.
            final Configuration conf = getConf();
            final FileSystem fs;
            try {
                fs = FileSystem.get(conf);
            } catch (final IOException e) {
                LOGGER.error("Exception getting filesystem: {}", e.getMessage());
                throw new OperationException("Failed to get filesystem from configuration: " + e.getMessage(), e);
            }


            writeSplits(fs, resultsFile, outputEveryNthRecord, expectedNumberOfSplits);

            try {
                fs.delete(resultsFile, true);
                LOGGER.info("Deleted the results file {}", resultsFile);
            } catch (final IOException e) {
                LOGGER.error("Failed to delete the results file {}", resultsFile);
                throw new OperationException("Failed to delete the results file: " + e.getMessage(), e);
            }
        }

        return SUCCESS_RESPONSE;
    }

    private void writeSplits(final FileSystem fs, final Path resultsFile, final long outputEveryNthRecord, final int numberSplitsExpected) throws OperationException {
        LOGGER.info("Writing splits to {}", operation.getSplitsFilePath());
        final Writable key = jobFactory.createKey();
        final Writable value = jobFactory.createValue();
        long count = 0;
        int numberSplitPointsOutput = 0;
        try (final SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(), Reader.file(resultsFile));
             final PrintStream splitsWriter = new PrintStream(
                     new BufferedOutputStream(fs.create(new Path(operation.getSplitsFilePath()), true)),
                     false, StandardCharsets.UTF_8.name())
        ) {
            while (numberSplitPointsOutput < numberSplitsExpected) {
                if (!reader.next(key, value)) {
                    break;
                }
                count++;
                if (count % outputEveryNthRecord == 0) {
                    final byte[] split = jobFactory.createSplit(key, value);
                    LOGGER.debug("Outputting split point number {} ({})",
                            numberSplitPointsOutput,
                            Base64.encodeBase64(split));
                    numberSplitPointsOutput++;
                    splitsWriter.println(new String(Base64.encodeBase64(split), StandardCharsets.UTF_8));
                }
            }
            LOGGER.info("Total number of records read was {}", count);
        } catch (final IOException e) {
            LOGGER.error("Exception reading results file and outputting split points: {}", e.getMessage());
            throw new OperationException(e.getMessage(), e);
        }
    }

}
