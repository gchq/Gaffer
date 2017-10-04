package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.partitioner.GafferKeyRangePartitioner;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.partitioner.GafferRangePartitioner;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer.AccumuloKeyValueReducer;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.hdfs.operation.partitioner.NoPartitioner;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AccumuloAddElementsFromHdfsJobFactoryTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public String inputDir;
    public String outputDir;
    public String splitsDir;
    public String splitsFile;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
    }

    @Test
    public void shouldSetupJob() throws IOException {
        // Given
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(splitsFile), true)))) {
            writer.write("1");
        }

        final AccumuloAddElementsFromHdfsJobFactory factory = new AccumuloAddElementsFromHdfsJobFactory();
        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .useProvidedSplits(true)
                .splitsFilePath(splitsFile)
                .build();
        final AccumuloStore store = mock(AccumuloStore.class);

        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        verify(job).setJarByClass(factory.getClass());
        verify(job).setJobName("Ingest HDFS data: Generator=" + TextMapperGeneratorImpl.class.getName() + ", output=" + outputDir);

        verify(job).setMapperClass(AddElementsFromHdfsMapper.class);
        verify(job).setMapOutputKeyClass(Key.class);
        verify(job).setMapOutputValueClass(Value.class);

        verify(job).setCombinerClass(AccumuloKeyValueReducer.class);

        verify(job).setReducerClass(AccumuloKeyValueReducer.class);
        verify(job).setOutputKeyClass(Key.class);
        verify(job).setOutputValueClass(Value.class);

        job.setOutputFormatClass(AccumuloFileOutputFormat.class);
        assertEquals(fs.makeQualified(new Path(outputDir)).toString(), job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));

        verify(job).setNumReduceTasks(2);
        verify(job).setPartitionerClass(GafferKeyRangePartitioner.class);
        assertEquals(splitsFile, job.getConfiguration().get(GafferRangePartitioner.class.getName() + ".cutFile"));
    }

    @Test
    public void shouldSetupAccumuloPartitionerWhenSetupJobAndPartitionerFlagIsTrue() throws IOException {
        shouldSetupAccumuloPartitionerWhenSetupJobForGivenPartitioner(GafferKeyRangePartitioner.class);
    }

    @Test
    public void shouldSetupAccumuloPartitionerWhenSetupJobAndPartitionerIsNull() throws IOException {
        shouldSetupAccumuloPartitionerWhenSetupJobForGivenPartitioner(null);
    }

    @Test
    public void shouldNotSetupAccumuloPartitionerWhenSetupJobAndPartitionerFlagIsFalse() throws IOException {
        shouldSetupAccumuloPartitionerWhenSetupJobForGivenPartitioner(NoPartitioner.class);
    }

    @Test
    public void shouldSetNoMoreThanMaxNumberOfReducersSpecified() throws IOException, StoreException, OperationException {
        // Given
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(StreamUtil.storeProps(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        store.initialise("graphId", schema, properties);
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStore splitTable = new SplitStore.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, store.createContext(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = new AccumuloAddElementsFromHdfsJobFactory();
        final Job job = Job.getInstance(localConf);

        // When
        AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .maxReducers(10)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() <= 10);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                .maxReducers(100)
                .splitsFilePath("target/data/splits.txt")
                .build();
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        assertTrue(job.getNumReduceTasks() <= 100);

        // When
        operation = new AddElementsFromHdfs.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
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
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(StreamUtil.storeProps(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        store.initialise("graphId", schema, properties);
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStore splitTable = new SplitStore.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable,  store.createContext(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = new AccumuloAddElementsFromHdfsJobFactory();
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
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        final AccumuloProperties properties = AccumuloProperties
                .loadStoreProperties(StreamUtil.storeProps(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        store.initialise("graphId", schema, properties);
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        final BufferedWriter writer = new BufferedWriter(new FileWriter(splitsFile));
        for (int i = 100; i < 200; i++) {
            writer.write(i + "\n");
        }
        writer.close();
        final SplitStore splitTable = new SplitStore.Builder()
                .inputPath(splitsFile)
                .build();
        store.execute(splitTable, store.createContext(new User()));
        final AccumuloAddElementsFromHdfsJobFactory factory = new AccumuloAddElementsFromHdfsJobFactory();
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

    private void shouldSetupAccumuloPartitionerWhenSetupJobForGivenPartitioner(final Class<? extends Partitioner> partitioner) throws IOException {
        // Given
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));
        fs.mkdirs(new Path(splitsDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(splitsFile), true)))) {
            writer.write("1");
        }

        final AccumuloAddElementsFromHdfsJobFactory factory = new AccumuloAddElementsFromHdfsJobFactory();
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
}
