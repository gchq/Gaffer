/*
 * Copyright 2016-2018 Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Store;

import java.util.List;

public class AddElementsFromHdfsTool extends Configured implements Tool {
    public static final int SUCCESS_RESPONSE = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsFromHdfsTool.class);

    private final AddElementsFromHdfs operation;
    private final Store store;
    private final AddElementsFromHdfsJobFactory jobFactory;
    private final Configuration config = new Configuration();

    public AddElementsFromHdfsTool(final AddElementsFromHdfsJobFactory jobFactory, final AddElementsFromHdfs operation, final Store store) {
        this.operation = operation;
        this.store = store;
        this.jobFactory = jobFactory;
    }

    @Override
    public int run(final String[] strings) throws Exception {
        jobFactory.prepareStore(store);
        LOGGER.info("Adding elements from HDFS");
        final List<Job> jobs = jobFactory.createJobs(operation, store);
        for (final Job job : jobs) {
            job.waitForCompletion(true);
            if (!job.isSuccessful()) {
                LOGGER.error("Error running job");
                throw new OperationException("Error running job");
            }
        }
        LOGGER.info("Finished adding elements from HDFS");

        return SUCCESS_RESPONSE;
    }

    public Configuration getConfig() {
        return config;
    }
}
