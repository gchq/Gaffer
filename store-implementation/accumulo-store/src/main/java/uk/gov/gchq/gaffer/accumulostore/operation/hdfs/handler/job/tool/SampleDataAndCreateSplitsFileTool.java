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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.tool;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory.SampleDataForSplitPointsJobFactory;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;


public class SampleDataAndCreateSplitsFileTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataAndCreateSplitsFileTool.class);
    public static final int SUCCESS_RESPONSE = 1;

    private final SampleDataForSplitPoints operation;
    private final AccumuloStore store;
    private Job job;

    public SampleDataAndCreateSplitsFileTool(final SampleDataForSplitPoints operation, final AccumuloStore store) {
        this.operation = operation;
        this.store = store;
    }

    @Override
    public int run(final String[] strings) throws OperationException {
        try {
            LOGGER.info("Creating job using SampleDataForSplitPointsJobFactory");
            job = new SampleDataForSplitPointsJobFactory().createJob(operation, store);
        } catch (final IOException e) {
            LOGGER.error("Failed to create Hadoop job: {}", e.getMessage());
            throw new OperationException("Failed to create the Hadoop job: " + e.getMessage(), e);
        }
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
            counter = job.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS);
            LOGGER.info("Number of records output = {}", counter);
        } catch (final IOException e) {
            LOGGER.error("Failed to get counter org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS from job: {}", e.getMessage());
            throw new OperationException("Failed to get counter: " + Task.Counter.REDUCE_OUTPUT_RECORDS, e);
        }

        int numberTabletServers;
        try {
            numberTabletServers = store.getConnection().instanceOperations().getTabletServers().size();
            LOGGER.info("Number of tablet servers is {}", numberTabletServers);
        } catch (final StoreException e) {
            LOGGER.error("Exception thrown getting number of tablet servers: {}", e.getMessage());
            throw new OperationException(e.getMessage(), e);
        }

        long outputEveryNthRecord = counter.getValue() / (numberTabletServers - 1);
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
        LOGGER.info("Writing splits to {}", operation.getResultingSplitsFilePath());
        final Key key = new Key();
        final Value value = new Value();
        long count = 0;
        int numberSplitPointsOutput = 0;
        try (final SequenceFile.Reader reader = new SequenceFile.Reader(fs, resultsFile, conf);
             final PrintStream splitsWriter = new PrintStream(
                     new BufferedOutputStream(fs.create(new Path(operation.getResultingSplitsFilePath()), true)),
                     false, CommonConstants.UTF_8)
        ) {
            while (reader.next(key, value) && numberSplitPointsOutput < numberTabletServers - 1) {
                count++;
                if (count % outputEveryNthRecord == 0) {
                    LOGGER.debug("Outputting split point number {} ({})",
                            numberSplitPointsOutput,
                            Base64.encodeBase64(key.getRow().getBytes()));
                    numberSplitPointsOutput++;
                    splitsWriter.println(new String(Base64.encodeBase64(key.getRow().getBytes()), CommonConstants.UTF_8));
                }
            }
            LOGGER.info("Total number of records read was {}", count);
        } catch (final IOException e) {
            LOGGER.error("Exception reading results file and outputting split points: {}", e.getMessage());
            throw new OperationException(e.getMessage(), e);
        }

        try {
            fs.delete(resultsFile, true);
            LOGGER.info("Deleted the results file {}", resultsFile);
        } catch (final IOException e) {
            LOGGER.error("Failed to delete the results file {}", resultsFile);
            throw new OperationException("Failed to delete the results file: " + e.getMessage(), e);
        }

        return SUCCESS_RESPONSE;
    }

}
