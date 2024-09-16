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

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
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

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper.SampleDataForSplitPointsMapper;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer.AccumuloKeyValueReducer;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.SampleDataForSplitPointsJobFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AccumuloSampleDataForSplitPointsJobFactory implements SampleDataForSplitPointsJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSampleDataForSplitPointsJobFactory.class);
    private final Configuration configuration;

    public AccumuloSampleDataForSplitPointsJobFactory(final Configuration configuration) {
        this.configuration = configuration;
    }

    public AccumuloSampleDataForSplitPointsJobFactory() {
        this(new Configuration());
    }

    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "key should always be an instance of Key")
    public byte[] createSplit(final Writable key, final Writable value) {
        return ((Key) key).getRow().getBytes();
    }

    @Override
    public Writable createKey() {
        return new Key();
    }

    @Override
    public Writable createValue() {
        return new Value();
    }

    @Override
    public int getExpectedNumberOfSplits(final Store store) {
        final AccumuloStore accumuloStore = (AccumuloStore) store;

        int numberTabletServers;
        try {
            numberTabletServers = accumuloStore.getTabletServers().size();
            LOGGER.info("Number of tablet servers is {}", numberTabletServers);
        } catch (final StoreException e) {
            LOGGER.error("Exception thrown getting number of tablet servers: {}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

        return numberTabletServers - 1;
    }

    @Override
    public JobConf createJobConf(final SampleDataForSplitPoints operation, final String mapperGeneratorClassName, final Store store) throws IOException {
        final JobConf jobConf = new JobConf(configuration);

        LOGGER.info("Setting up job conf");
        jobConf.set(SCHEMA, new String(store.getSchema().toCompactJson(), StandardCharsets.UTF_8));
        LOGGER.info("Added {} {} to job conf", SCHEMA, new String(store.getSchema().toCompactJson(), StandardCharsets.UTF_8));
        jobConf.set(MAPPER_GENERATOR, mapperGeneratorClassName);
        LOGGER.info("Added {} of {} to job conf", MAPPER_GENERATOR, mapperGeneratorClassName);
        jobConf.set(VALIDATE, String.valueOf(operation.isValidate()));
        LOGGER.info("Added {} option of {} to job conf", VALIDATE, operation.isValidate());
        jobConf.set(PROPORTION_TO_SAMPLE, String.valueOf(operation.getProportionToSample()));
        LOGGER.info("Added {} option of {} to job conf", PROPORTION_TO_SAMPLE, String.valueOf(operation.getProportionToSample()));

        final Integer numTasks = operation.getNumMapTasks();

        if (null != numTasks) {
            jobConf.setNumMapTasks(numTasks);
            LOGGER.info("Set number of map tasks to {} on job conf", numTasks);
        }

        jobConf.setNumReduceTasks(1);
        LOGGER.info("Set number of reduce tasks to 1 on job conf");

        jobConf.set(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ((AccumuloStore) store).getKeyPackage().getKeyConverter().getClass().getName());

        return jobConf;
    }

    @Override
    public void setupJob(final Job job, final SampleDataForSplitPoints operation, final String mapperGeneratorClassName, final Store store) throws IOException {
        job.setJarByClass(getClass());
        job.setJobName(getJobName(mapperGeneratorClassName, new Path(operation.getOutputPath())));

        setupMapper(job);
        setupReducer(job);

        setupOutput(job, operation, store);
    }

    protected String getJobName(final String mapperGenerator, final Path outputPath) {
        return "Sample Data: Generator=" + mapperGenerator + ", output=" + outputPath;
    }

    protected void setupMapper(final Job job) throws IOException {
        job.setMapperClass(SampleDataForSplitPointsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
    }

    protected void setupReducer(final Job job)
            throws IOException {
        job.setReducerClass(AccumuloKeyValueReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
    }

    protected void setupOutput(final Job job, final SampleDataForSplitPoints operation, final Store store) throws IOException {
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
