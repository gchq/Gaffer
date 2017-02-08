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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory.AccumuloAddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.operation.OperationException;
import java.io.IOException;

public class FetchElementsFromHdfsTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetchElementsFromHdfsTool.class);
    public static final int SUCCESS_RESPONSE = 1;

    private final AddElementsFromHdfs operation;
    private final AccumuloStore store;

    public FetchElementsFromHdfsTool(final AddElementsFromHdfs operation, final AccumuloStore store) {
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
            LOGGER.error("Error running job");
            throw new OperationException("Error running job");
        }
        LOGGER.info("Finished adding elements from HDFS");

        return SUCCESS_RESPONSE;
    }

    private void checkHdfsDirectories(final AddElementsFromHdfs operation) throws IOException {
        LOGGER.info("Checking that the correct HDFS directories exist");
        final FileSystem fs = FileSystem.get(getConf());

        final Path outputPath = new Path(operation.getOutputPath());
        LOGGER.info("Ensuring output directory {} doesn't exist", outputPath);
        if (fs.exists(outputPath)) {
            if (fs.listFiles(outputPath, true).hasNext()) {
                LOGGER.error("Output directory exists and is not empty: {}", outputPath);
                throw new IllegalArgumentException("Output directory exists and is not empty: " + outputPath);
            }
            LOGGER.info("Output directory exists and is empty so deleting: {}", outputPath);
            fs.delete(outputPath, true);
        }

    }
}
