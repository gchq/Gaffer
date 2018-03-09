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
package uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public interface JobFactory<O extends MapReduce> {
    String SCHEMA = "schema";
    String MAPPER_GENERATOR = "mapperGenerator";
    String VALIDATE = "validate";

    /**
     * Creates a job with the store specific job initialisation and then applies the operation specific
     * {@link uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser}.
     *
     * @param operation The operation.
     * @param store     The store executing the operation.
     * @return The created job.
     * @throws IOException for IO issues.
     */
    default List<Job> createJobs(final O operation, final Store store) throws IOException {
        final List<Job> jobs = new ArrayList<>();
        Map<String, List<String>> mapperGeneratorsToInputPathsList = new HashMap<>();
        for (final Map.Entry<String, String> entry : operation.getInputMapperPairs().entrySet()) {
            if (mapperGeneratorsToInputPathsList.containsKey(entry.getValue())) {
                mapperGeneratorsToInputPathsList.get(entry.getValue()).add(entry.getKey());
            } else {
                mapperGeneratorsToInputPathsList.put(entry.getValue(), Lists.newArrayList(entry.getKey()));
            }
        }

        for (final String mapperGeneratorClassName : mapperGeneratorsToInputPathsList.keySet()) {
            final JobConf jobConf = createJobConf(operation, mapperGeneratorClassName, store);
            final Job job = Job.getInstance(jobConf);
            setupJob(job, operation, mapperGeneratorClassName, store);

            if (null != operation.getJobInitialiser()) {
                operation.getJobInitialiser().initialiseJob(job, operation, store);
            }
            jobs.add(job);
        }
        return jobs;
    }

    /**
     * Creates an {@link JobConf} to be used for the add from hdfs.
     *
     * @param operation                The Operation.
     * @param mapperGeneratorClassName Class name for the MapperGenerator class.
     * @param store                    The store.
     * @return The JobConf.
     * @throws IOException For IO issues.
     */
    JobConf createJobConf(final O operation, final String mapperGeneratorClassName, final Store store) throws IOException;

    /**
     * Sets up all parts of the Job to be used on the add from hdfs.
     *
     * @param job             The {@link Job} to be executed.
     * @param operation       The Operation.
     * @param mapperGenerator Class Name for the MapperGenerator class.
     * @param store           The store.
     * @throws IOException For IO issues.
     */
    void setupJob(final Job job, final O operation, final String mapperGenerator, final Store store) throws IOException;
}
