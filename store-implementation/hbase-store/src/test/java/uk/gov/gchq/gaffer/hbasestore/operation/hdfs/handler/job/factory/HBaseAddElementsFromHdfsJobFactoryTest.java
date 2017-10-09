package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.job.factory;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.SingleUseMiniHBaseStore;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HBaseAddElementsFromHdfsJobFactoryTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private String inputDir;
    private String outputDir;
    private String stagingDir;
    private String failureDir;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "/inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        failureDir = testFolder.getRoot().getAbsolutePath() + "/failureDir";
        stagingDir = testFolder.getRoot().getAbsolutePath() + "/stagingDir";
    }

    @Test
    public void shouldSetupJob() throws IOException, StoreException {
        // Given
        final JobConf localConf = createLocalConf();
        final FileSystem fs = FileSystem.getLocal(localConf);
        fs.mkdirs(new Path(outputDir));

        final HBaseAddElementsFromHdfsJobFactory factory = new HBaseAddElementsFromHdfsJobFactory();
        final Job job = mock(Job.class);
        final AddElementsFromHdfs operation = new AddElementsFromHdfs.Builder()
                .addInputMapperPair(new Path(inputDir).toString(), TextMapperGeneratorImpl.class.getName())
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .option(HBaseStoreConstants.OPERATION_HDFS_STAGING_PATH, stagingDir)
                .build();

        final HBaseStore store = new SingleUseMiniHBaseStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));
        final HBaseProperties properties = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        store.initialise("graphId", schema, properties);

        given(job.getConfiguration()).willReturn(localConf);

        // When
        factory.setupJob(job, operation, TextMapperGeneratorImpl.class.getName(), store);

        // Then
        verify(job).setJarByClass(factory.getClass());
        verify(job).setJobName("Ingest HDFS data: Generator=" + TextMapperGeneratorImpl.class.getName() + ", output=" + outputDir);

        verify(job).setMapperClass(AddElementsFromHdfsMapper.class);
        verify(job).setMapOutputKeyClass(ImmutableBytesWritable.class);
        verify(job).setMapOutputValueClass(Put.class);

        verify(job).setReducerClass(PutSortReducer.class);
        verify(job).setOutputKeyClass(ImmutableBytesWritable.class);
        verify(job).setOutputValueClass(KeyValue.class);
        verify(job).setOutputFormatClass(HFileOutputFormat2.class);

        assertEquals(fs.makeQualified(new Path(outputDir)).toString(), job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));

        verify(job).setNumReduceTasks(1);
    }

    private JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        conf.set(JTConfig.JT_IPC_ADDRESS, JTConfig.LOCAL_FRAMEWORK_NAME);

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
