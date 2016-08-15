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
import gaffer.accumulostore.operation.hdfs.handler.job.AccumuloAddElementsFromHdfsJobFactory;
import gaffer.accumulostore.utils.IngestUtils;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.operation.OperationException;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class FetchElementsFromHdfs extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchElementsFromHdfs.class);
    public static final int SUCCESS_RESPONSE = 1;

    private final AddElementsFromHdfs operation;
    private final AccumuloStore store;

    public FetchElementsFromHdfs(final AddElementsFromHdfs operation, final AccumuloStore store) {
        this.operation = operation;
        this.store = store;
    }

    @Override
    public int run(final String[] strings) throws Exception {
        checkHdfsDirectories(operation);

        LOGGER.info("Ensuring table {} exists", store.getProperties().getTable());
        TableUtils.ensureTableExists(store);

        LOGGER.info("Adding elements from HDFS");
        final Job job = new AccumuloAddElementsFromHdfsJobFactory().createJob(operation, store);
        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            throw new OperationException("Error running job");
        }

        return SUCCESS_RESPONSE;
    }

    private void checkHdfsDirectories(final AddElementsFromHdfs operation) throws IOException {
        final FileSystem fs = FileSystem.get(getConf());

        final Path outputPath = new Path(operation.getOutputPath());
        if (fs.exists(outputPath)) {
            if (fs.listFiles(outputPath, true).hasNext()) {
                throw new IllegalArgumentException("Output directory is not empty: " + outputPath);
            }
            fs.delete(outputPath, true);
        }

        final Path failurePath = new Path(operation.getFailurePath());
        if (fs.exists(failurePath)) {
            if (fs.listFiles(failurePath, true).hasNext()) {
                throw new IllegalArgumentException("Failure directory is not empty: " + failurePath);
            }
        } else {
            fs.mkdirs(failurePath);
        }
        IngestUtils.setDirectoryPermsForAccumulo(fs, failurePath);
    }
}
