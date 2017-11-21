/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.integration;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AddElementsFromHdfsIT {
    private static final String VERTEX_ID_PREFIX = "vertexId";
    public static final int NUM_ELEMENTS = 10;
    public static final int DUPLICATES = 4;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private String inputDir;
    public String outputDir;
    public String stagingDir;
    public String failureDir;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "/inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        failureDir = testFolder.getRoot().getAbsolutePath() + "/failureDir";
        stagingDir = testFolder.getRoot().getAbsolutePath() + "/stagingDir";
    }

    @Test
    public void shouldAddElementsFromHdfs() throws Exception {
        addElementsFromHdfs(getSchema(), true);
    }

    @Test
    public void shouldAddElementsFromHdfsWithNoAggregation() throws Exception {
        final Schema defaultSchema = getSchema();
        final SchemaEdgeDefinition defaultEdge1 = defaultSchema.getEdge(TestGroups.EDGE);
        final Schema schema = new Schema.Builder()
                .merge(defaultSchema)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(defaultEdge1.getSource())
                        .destination(defaultEdge1.getDestination())
                        .directed(defaultEdge1.getDirected())
                        .properties(defaultEdge1.getPropertyMap())
                        .aggregate(false)
                        .build())
                .build();
        addElementsFromHdfs(schema, false);
    }

    @Test
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenOutputDirectoryContainsFiles() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        try {
            addElementsFromHdfs(getSchema(), true);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertEquals("Output directory file:" + outputDir + " already exists", e.getCause().getMessage());
        }
    }

    private void addElementsFromHdfs(final Schema schema, final boolean fullyAggregated) throws Exception {
        // Given
        createInputFile();
        final Graph graph = createGraph(schema);

        // When
        graph.execute(new AddElementsFromHdfs.Builder()
                .addInputMapperPair(new Path(inputDir).toString(), TextMapperGeneratorImpl.class.getName())
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .option(HBaseStoreConstants.OPERATION_HDFS_STAGING_PATH, stagingDir)
                .build(), new User());

        // Then
        final CloseableIterable<? extends Element> elements = graph.execute(new GetAllElements(), new User());
        final List<Element> expectedElements = new ArrayList<>(NUM_ELEMENTS);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            expectedElements.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(VERTEX_ID_PREFIX + i)
                    .property(TestPropertyNames.COUNT, DUPLICATES)
                    .property(TestPropertyNames.VISIBILITY, "")
                    .build());
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(VERTEX_ID_PREFIX + i)
                    .dest(VERTEX_ID_PREFIX + (i + 1))
                    .directed(true)
                    .property(TestPropertyNames.COUNT, DUPLICATES)
                    .property(TestPropertyNames.VISIBILITY, "")
                    .build();
            if (fullyAggregated) {
                expectedElements.add(edge);
            } else {
                edge.putProperty(TestPropertyNames.COUNT, 1);
                for (int j = 0; j < DUPLICATES; j++) {
                    expectedElements.add(edge);
                }
            }
        }
        ElementUtil.assertElementEquals(expectedElements, elements);
    }

    private void createInputFile() throws IOException, StoreException {
        final Path inputPath = new Path(inputDir);
        final Path inputFilePath = new Path(inputDir + "/file.txt");
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(inputPath);

        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath, true)))) {
            for (int duplicates = 0; duplicates < DUPLICATES; duplicates++) {
                for (int i = 0; i < NUM_ELEMENTS; i++) {
                    writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
                    writer.write(TestGroups.EDGE + "," + VERTEX_ID_PREFIX + i + "," + VERTEX_ID_PREFIX + (i + 1) + "\n");
                }
            }
        }
    }

    private JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        conf.set(JTConfig.JT_IPC_ADDRESS, JTConfig.LOCAL_FRAMEWORK_NAME);

        return conf;
    }

    private Graph createGraph(final Schema schema) throws StoreException {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(schema)
                .build();
    }

    private Schema getSchema() {
        return Schema.fromJson(StreamUtil.schemas(getClass()));
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
            if (2 == parts.length) {
                return new Entity.Builder()
                        .group(parts[0])
                        .vertex(parts[1])
                        .property(TestPropertyNames.COUNT, 1)
                        .build();
            }

            return new Edge.Builder()
                    .group(parts[0])
                    .source(parts[1])
                    .dest(parts[2])
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 1)
                    .build();
        }
    }
}
