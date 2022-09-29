/*
 * Copyright 2016-2022 Crown Copyright
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

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.partitioner.GafferKeyRangePartitioner;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.partitioner.GafferRangePartitioner;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer.AccumuloKeyValueReducer;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.hdfs.operation.hander.job.factory.AbstractJobFactoryTest;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.hdfs.operation.partitioner.NoPartitioner;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AccumuloAddElementsFromHdfsJobFactoryTest extends AbstractJobFactoryTest {

    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloAddElementsFromHdfsJobFactoryTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloAddElementsFromHdfsJobFactoryTest.class));
    private AccumuloStore store = new SingleUseMiniAccumuloStore();
    final JobConf localConf = createLocalConf();

    public AccumuloAddElementsFromHdfsJobFactory factory;
    public FileSystem fs;
    public String inputDir;
    public String outputDir;
    public String splitsDir;
    public String splitsFile;

    @BeforeEach
    public void setup(@TempDir java.nio.file.Path tempDir) throws IOException {
        inputDir = new File(tempDir.toString(), "inputDir").getAbsolutePath();
        outputDir = new File(tempDir.toString(), "outputDir").getAbsolutePath();
        splitsDir = new File(tempDir.toString(), "splitsDir").getAbsolutePath();
        splitsFile = new File(splitsDir, "splits").getAbsolutePath();

        factory = getJobFactory();
        fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
    }


    @Test
    public void shouldSetupJob() throws IOException  {
        // Given
        writeOneToSplitsFile();

        final Job job = mock(Job.class);

        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile)
                .build();

        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        verify(job).setJarByClass(factory.getClass());
        verify(job).setJobName(String.format(AccumuloAddElementsFromHdfsJobFactory.INGEST_HDFS_DATA_GENERATOR_S_OUTPUT_S, TextMapperGeneratorImpl.class.getName(), outputDir));

        verify(job).setMapperClass(AddElementsFromHdfsMapper.class);
        verify(job).setMapOutputKeyClass(Key.class);
        verify(job).setMapOutputValueClass(Value.class);

        verify(job).setCombinerClass(AccumuloKeyValueReducer.class);

        verify(job).setReducerClass(AccumuloKeyValueReducer.class);
        verify(job).setOutputKeyClass(Key.class);
        verify(job).setOutputValueClass(Value.class);

        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        assertEquals(fs.makeQualified(new Path(outputDir)).toString(),
                job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));

        verify(job).setNumReduceTasks(2);
        verify(job).setPartitionerClass(GafferKeyRangePartitioner.class);

        if (IS_OS_WINDOWS) {
            alterPathForWindows(splitsFile);
        }

        assertEquals(splitsFile, job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));
    }

    @Test
    public void shouldSetupAccumuloPartitionerWhenSetupJobAndPartitionerFlagIsTrue() throws IOException {
        final Class<? extends Partitioner> partitioner = GafferKeyRangePartitioner.class;
        writeOneToSplitsFile();

        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .partitioner(partitioner)
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile)
                .build();
        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        verify(job).setNumReduceTasks(2);
        verify(job).setPartitionerClass(GafferKeyRangePartitioner.class);

        if (IS_OS_WINDOWS) {
            alterPathForWindows(splitsFile);
        }
        assertEquals(splitsFile, job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));

    }

    @Test
    public void shouldSetupAccumuloPartitionerWhenSetupJobAndPartitionerIsNull() throws IOException {
        writeOneToSplitsFile();

        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .partitioner(null)
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile)
                .build();
        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        verify(job).setNumReduceTasks(2);
        verify(job).setPartitionerClass(GafferKeyRangePartitioner.class);

        if (IS_OS_WINDOWS) {
            alterPathForWindows(splitsFile);
        }
        assertEquals(splitsFile, job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));
    }

    @Test
    public void shouldNotSetupAccumuloPartitionerWhenSetupJobAndPartitionerFlagIsFalse() throws IOException {
        //Given
        writeOneToSplitsFile();

        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .partitioner(NoPartitioner.class)
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile)
                .build();
        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        verify(job, never()).setNumReduceTasks(Mockito.anyInt());
        verify(job, never()).setPartitionerClass(Mockito.any(Class.class));
        assertNull(job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));

    }

    @Test
    public void shouldSetNoMoreThanMaxNumberOfReducersSpecified() throws IOException, StoreException, OperationException {
        // Given
        writeManyToSplitsFile();
        store.initialise("graphId", SCHEMA, PROPERTIES);
        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile.toString())
                .build();
        store.execute(splitTable, new Context(new User()));

        final Job job = Job.getInstance(localConf);

        // When
        AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir.toString())
                .addInputMapperPair(inputDir.toString(), TextMapperGeneratorImpl.class.getName())
                .maxReducers(10)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() <= 10);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir.toString())
                .addInputMapperPair(inputDir.toString(), TextMapperGeneratorImpl.class.getName())
                .maxReducers(100)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() <= 100);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir.toString())
                .addInputMapperPair(inputDir.toString(), TextMapperGeneratorImpl.class.getName())
                .maxReducers(1000)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() <= 1000);
    }

    @Test
    public void shouldSetNoLessThanMinNumberOfReducersSpecified() throws IOException, StoreException, OperationException {
        // Given
        store.initialise("graphId", SCHEMA, PROPERTIES);
        writeManyToSplitsFile();

        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, new Context(new User()));

        final Job job = Job.getInstance(localConf);

        // When
        AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(10)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() >= 10);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(100)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() >= 100);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(1000)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() >= 1000);
    }

    @Test
    public void shouldSetNumberOfReducersBetweenMinAndMaxSpecified() throws IOException, StoreException, OperationException {
        // Given
        store.initialise("graphId", SCHEMA, PROPERTIES);
        writeManyToSplitsFile();

        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, new Context(new User()));

        final Job job = Job.getInstance(localConf);

        // When
        AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(10)
                .maxReducers(20)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() >= 10);
        assertTrue(job.getNumReduceTasks() <= 20);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(100)
                .maxReducers(200)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() >= 100);
        assertTrue(job.getNumReduceTasks() <= 200);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(1000)
                .maxReducers(2000)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() >= 1000);
        assertTrue(job.getNumReduceTasks() <= 2000);
    }


    @Test
    public void shouldThrowExceptionWhenMaxReducersSetOutsideOfRange() throws IOException, StoreException, OperationException {
        // Given
        store.initialise("graphId", SCHEMA, PROPERTIES);
        writeManyToSplitsFile();

        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, new Context(new User()));

        final Job job = Job.getInstance(localConf);

        // When
        AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .minReducers(100)
                .maxReducers(101)
                .splitsFilePath("target/data/splits.txt")
                .build();

        // Then
        try {
            factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("not a valid range"));
        }
    }

    private JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.jobtracker.address", "local");

        return conf;
    }

    public static final class TextMapperGeneratorImpl extends TextMapperGenerator {
        private TextMapperGeneratorImpl() {
            super(new ExampleGenerator());
        }
    }

    public static final class ExampleGenerator implements OneToOneElementGenerator<String> {
        @Override
        public Element _apply(final String domainObject) {
            final String[] parts = domainObject.split(",");
            return new Entity(parts[0], parts[1]);
        }
    }

    @Override
    protected Store getStoreConfiguredWith(final Class<JSONSerialiser> jsonSerialiserClass, final String jsonSerialiserModules, final Boolean strictJson) throws IOException, StoreException {
        super.configureStoreProperties(PROPERTIES, jsonSerialiserClass, jsonSerialiserModules, strictJson);

        store.initialise("graphId", SCHEMA, PROPERTIES);

        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));

        return store;
    }

    @Override
    protected AccumuloAddElementsFromHdfsJobFactory getJobFactory() {
        return new AccumuloAddElementsFromHdfsJobFactory();
    }

    @Override
    protected MapReduce getMapReduceOperation() {
        return new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, JsonMapperGenerator.class.getName())
                .splitsFilePath(splitsFile)
                .build();
    }

    /*
     * The hadoop library alters Windows files paths, so they no longer work on Windows,
     * so the filepath needs to be altered if the tests are run on Windows
     */
    public void alterPathForWindows(String path) {
        String alteredPath = "/" + path.replace("\\", "/");
        splitsFile = alteredPath;
    }

    public void writeOneToSplitsFile() throws IOException {
        try (final BufferedWriter writer =
                new BufferedWriter(new OutputStreamWriter(fs.create(new Path(splitsFile), true)))) {
            writer.write("1");
            writer.close();
        }
    }

    public void writeManyToSplitsFile() throws IOException {
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile.toString()));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
    }

}
