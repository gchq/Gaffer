/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
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
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;
    private AccumuloStore store = new SingleUseAccumuloStore();

    public String inputDir;
    public String outputDir;
    public String splitsDir;
    public String splitsFile;

    @BeforeAll
    public static void setUpStore(@TempDir java.nio.file.Path tempDir) {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @BeforeEach
    public void setup(@TempDir java.nio.file.Path tempDir) {
        inputDir = new File(tempDir.toString(), "inputDir").getAbsolutePath();
        outputDir = new File(tempDir.toString(), "outputDir").getAbsolutePath();
        splitsDir = new File(tempDir.toString(), "splitsDir").getAbsolutePath();
        splitsFile = new File(splitsDir, "splits").getAbsolutePath();
    }

    @Test
    public void shouldSetupJob() throws IOException {
        // Given
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        try (final BufferedWriter writer =
                new BufferedWriter(new OutputStreamWriter(fs.create(new Path(splitsFile), true)))) {
            writer.write("1");
        }

        final AccumuloAddElementsFromHdfsJobFactory factory = getJobFactory();
        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir.toString())
                .addInputMapperPair(inputDir.toString(), TextMapperGeneratorImpl.class.getName())
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile.toString())
                .build();
        final AccumuloStore store = mock(AccumuloStore.class);

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
        assertEquals(splitsFile, job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));
    }

    @Test
    public void shouldSetupAccumuloPartitionerWhenSetupJobAndPartitionerFlagIsTrue() throws IOException {
        setupAccumuloPartitionerWithGivenPartitioner(GafferKeyRangePartitioner.class);
    }

    @Test
    public void shouldSetupAccumuloPartitionerWhenSetupJobAndPartitionerIsNull() throws IOException {
        setupAccumuloPartitionerWithGivenPartitioner(null);
    }

    @Test
    public void shouldNotSetupAccumuloPartitionerWhenSetupJobAndPartitionerFlagIsFalse() throws IOException {
        setupAccumuloPartitionerWithGivenPartitioner(NoPartitioner.class);
    }

    @Test
    public void shouldSetNoMoreThanMaxNumberOfReducersSpecified() throws IOException, StoreException, OperationException {
        // Given
        store.initialise("graphId", SCHEMA, PROPERTIES);
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile.toString()));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile.toString())
                .build();
        store.execute(splitTable, new Context(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = getJobFactory();
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
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, new Context(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = getJobFactory();
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
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, new Context(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = getJobFactory();
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
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStoreFromFile splitTable = new SplitStoreFromFile.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, new Context(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = getJobFactory();
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

    private void setupAccumuloPartitionerWithGivenPartitioner(final Class<? extends Partitioner> partitioner) throws IOException {
        // Given
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        try (final BufferedWriter writer =
                new BufferedWriter(new OutputStreamWriter(fs.create(new Path(splitsFile))))) {
            writer.write("1");
        }

        final AccumuloAddElementsFromHdfsJobFactory factory = getJobFactory();
        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .partitioner(partitioner)
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile)
                .build();
        final AccumuloStore store = mock(AccumuloStore.class);
        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        if (NoPartitioner.class.equals(partitioner)) {
            verify(job, never()).setNumReduceTasks(Mockito.anyInt());
            verify(job, never()).setPartitionerClass(Mockito.any(Class.class));
            assertNull(job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));
        } else {
            verify(job).setNumReduceTasks(2);
            verify(job).setPartitionerClass(GafferKeyRangePartitioner.class);
            assertEquals(splitsFile, job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));
        }
    }

    private JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.jobtracker.address", "local");

        return conf;
    }

    private java.nio.file.Path Paths(java.nio.file.Path tempDir, String inputDir) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static final class TextMapperGeneratorImpl extends TextMapperGenerator {
        public TextMapperGeneratorImpl() {
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

        final JobConf localConf = createLocalConf();
        final FileSystem fileSystem = FileSystem.getLocal(localConf);
        fileSystem.mkdirs(new Path(outputDir));
        fileSystem.mkdirs(new Path(splitsDir));

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
}
