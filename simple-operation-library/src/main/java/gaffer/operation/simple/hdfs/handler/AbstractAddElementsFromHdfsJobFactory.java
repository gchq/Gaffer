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
package gaffer.operation.simple.hdfs.handler;

import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.Store;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public abstract class AbstractAddElementsFromHdfsJobFactory implements AddElementsFromHdfsJobFactory {

    /**
     * Creates a job with the store specific job initialisation and then applies the operation specific
     * {@link gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser}.
     *
     * @param operation the add elements from hdfs operation
     * @param store     the store executing the operation
     * @return the created job
     * @throws IOException for IO issues
     */
    public Job createJob(final AddElementsFromHdfs operation, final Store store) throws IOException {
        final JobConf jobConf = createJobConf(operation, store);
        setupJobConf(jobConf, operation, store);

        final Job job = Job.getInstance(jobConf);
        setupJob(job, operation, store);

        // Apply Operation Specific Job Configuration
        if (null != operation.getJobInitialiser()) {
            operation.getJobInitialiser().initialiseJob(job, operation, store);
        }

        return job;
    }

    protected JobConf createJobConf(final AddElementsFromHdfs operation, final Store store) throws IOException {
        return new JobConf(new Configuration());
    }

    protected void setupJobConf(final JobConf jobConf, final AddElementsFromHdfs operation, final Store store) throws IOException {
        jobConf.set(DATA_SCHEMA, new String(store.getDataSchema().toJson(false), UTF_8_CHARSET));
        jobConf.set(STORE_SCHEMA, new String(store.getStoreSchema().toJson(false), UTF_8_CHARSET));
        jobConf.set(MAPPER_GENERATOR, operation.getMapperGeneratorClassName());
        jobConf.set(VALIDATE, String.valueOf(operation.isValidate()));
        Integer numTasks = operation.getNumMapTasks();
        if (null != numTasks) {
            jobConf.setNumMapTasks(numTasks);
        }
        numTasks = operation.getNumReduceTasks();
        if (null != numTasks) {
            jobConf.setNumReduceTasks(numTasks);
        }
    }

    protected void setupJob(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        job.setJarByClass(getClass());
        job.setJobName(getJobName(operation.getInputPath(), operation.getOutputPath()));
    }

    protected String getJobName(final Path inputPath, final Path outputPath) {
        return "Ingest HDFS data: input=" + inputPath + ", output=" + outputPath;
    }
}
