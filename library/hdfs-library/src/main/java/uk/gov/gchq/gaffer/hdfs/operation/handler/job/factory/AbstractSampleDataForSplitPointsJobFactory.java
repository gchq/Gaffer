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
package uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.store.Store;
import java.io.IOException;

public abstract class AbstractSampleDataForSplitPointsJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSampleDataForSplitPointsJobFactory.class);

    public static final String PROPORTION_TO_SAMPLE = "proportion_to_sample";
    public static final String SCHEMA = "schema";
    public static final String MAPPER_GENERATOR = "mapperGenerator";
    public static final String VALIDATE = "validate";

    public abstract long getOutputEveryNthRecord(final Store store, final long totalNumber);

    public abstract byte[] createSplit(final Writable key, final Writable value);

    public abstract Writable createKey();

    public abstract Writable createValue();

    public abstract int getExpectedNumberOfSplits(final Store store);

    /**
     * Creates a job with the store specific job initialisation and then applies the operation specific
     * {@link uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser}.
     *
     * @param operation the add elements from hdfs operation
     * @param store     the store executing the operation
     * @return the created job
     * @throws IOException for IO issues
     */
    public Job createJob(final SampleDataForSplitPoints operation, final Store store) throws IOException {
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

    protected JobConf createJobConf(final SampleDataForSplitPoints operation, final Store store) throws IOException {
        return new JobConf(new Configuration());
    }

    protected void setupJobConf(final JobConf jobConf, final SampleDataForSplitPoints operation, final Store store) throws IOException {
        jobConf.set(SCHEMA, new String(store.getSchema().toCompactJson(), CommonConstants.UTF_8));
        jobConf.set(MAPPER_GENERATOR, operation.getMapperGeneratorClassName());
        jobConf.set(VALIDATE, String.valueOf(operation.isValidate()));
        jobConf.set(PROPORTION_TO_SAMPLE, String.valueOf(operation.getProportionToSample()));
        Integer numTasks = operation.getNumMapTasks();
        if (null != numTasks) {
            jobConf.setNumMapTasks(numTasks);
        }
        jobConf.setNumReduceTasks(1);
    }

    protected void setupJob(final Job job, final SampleDataForSplitPoints operation, final Store store) throws IOException {
        job.setJarByClass(getClass());
        job.setJobName(getJobName(operation.getMapperGeneratorClassName(), new Path(operation.getOutputPath())));
        setupOutput(job, operation, store);
    }


    protected String getJobName(final String mapperGenerator, final Path outputPath) {
        return "Sample Data: Generator=" + mapperGenerator + ", output=" + outputPath;
    }

    private void setupOutput(final Job job, final SampleDataForSplitPoints operation, final Store store) throws IOException {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(operation.getOutputPath()));
        if (null != operation.getCompressionCodec()) {
            if (GzipCodec.class.isAssignableFrom(operation.getCompressionCodec()) && !NativeCodeLoader.isNativeCodeLoaded() && !ZlibFactory.isNativeZlibLoaded(job.getConfiguration())) {
                LOGGER.warn("SequenceFile doesn't work with GzipCodec without native-hadoop code!");
            } else {
                SequenceFileOutputFormat.setCompressOutput(job, true);
                SequenceFileOutputFormat.setOutputCompressorClass(job, operation.getCompressionCodec());
                SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            }
        }
    }
}
