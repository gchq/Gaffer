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
package gaffer.accumulostore.operation.hdfs.handler.tool;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.operation.hdfs.handler.job.SampleDataForSplitPointsJobFactory;
import gaffer.accumulostore.operation.hdfs.impl.SampleDataForSplitPoints;
import gaffer.commonutil.CommonConstants;
import gaffer.operation.OperationException;
import gaffer.store.StoreException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;


public class SampleDataAndCreateSplitsFileTool extends Configured implements Tool {

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
            job = new SampleDataForSplitPointsJobFactory().createJob(operation, store);
        } catch (IOException e) {
            throw new OperationException("Failed to create the hadoop job : " + e.getMessage(), e);
        }
        try {
            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw new OperationException("Erorr while waiting for job to complete : " + e.getMessage(), e);
        }

        try {
            if (!job.isSuccessful()) {
                throw new OperationException("Error running job");
            }
        } catch (IOException e) {
            throw new OperationException("Error running job" + e.getMessage(), e);
        }

        // Number of records output
        // NB In the following line use mapred.Task.Counter.REDUCE_OUTPUT_RECORDS rather than
        // mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS as this is more compatible with earlier
        // versions of Hadoop.
        Counter counter;
        try {
            counter = job.getCounters().findCounter(org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS);
        } catch (IOException e) {
            throw new OperationException("Failed to get counter: " + org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS, e);
        }

        int numberTabletServers;
        try {
            numberTabletServers = store.getConnection().instanceOperations().getTabletServers().size();
        } catch (StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }

        long outputEveryNthRecord = counter.getValue() / (numberTabletServers - 1);

        // Read through resulting file, pick out the split points and write to file.
        Configuration conf = getConf();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new OperationException("Failed to get Filesystem from configuraiton : " + e.getMessage(), e);
        }
        Path resultsFile = new Path(operation.getOutputPath(), "part-r-00000");
        Key key = new Key();
        Value value = new Value();
        long count = 0;
        int numberSplitPointsOutput = 0;
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, resultsFile, conf);
             PrintStream splitsWriter = new PrintStream(new BufferedOutputStream(fs.create(new Path(operation.getResultingSplitsFilePath()), true)), false, CommonConstants.UTF_8)
        ) {
            while (reader.next(key, value) && numberSplitPointsOutput < numberTabletServers - 1) {
                count++;
                if (count % outputEveryNthRecord == 0) {
                    numberSplitPointsOutput++;
                    splitsWriter.println(new String(Base64.encodeBase64(key.getRow().getBytes()), CommonConstants.UTF_8));
                }
            }
        } catch (IOException e) {
            throw new OperationException(e.getMessage(), e);
        }

        try {
            fs.delete(resultsFile, true);
        } catch (IOException e) {
            throw new OperationException("Failed to delete the mapreduce result file : " + e.getMessage(), e);
        }

        return SUCCESS_RESPONSE;
    }

}
