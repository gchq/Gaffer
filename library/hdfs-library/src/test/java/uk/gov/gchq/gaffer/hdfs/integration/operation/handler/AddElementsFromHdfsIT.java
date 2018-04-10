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

package uk.gov.gchq.gaffer.hdfs.integration.operation.handler;

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
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AddElementsFromHdfsIT {
    private static final String VERTEX_ID_PREFIX = "vertexId";
    public static final int NUM_ELEMENTS = 100;
    public static final int DUPLICATES = 4;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected String inputDir;
    protected String outputDir;
    protected String failureDir;
    protected String splitsDir;
    protected String splitsFile;
    protected String workingDir;
    protected String stagingDir;

    protected abstract Graph createGraph(final Schema schema) throws Exception;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "/inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        failureDir = testFolder.getRoot().getAbsolutePath() + "/failureDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
        workingDir = testFolder.getRoot().getAbsolutePath() + "/workingDir";
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
    public void shouldAddElementsFromHdfsWhenOutputDirectoryAlreadyExists() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));

        addElementsFromHdfs(getSchema(), true);
    }

    @Test
    public void shouldAddElementsFromHdfsWhenFailureDirectoryAlreadyExists() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(failureDir));

        addElementsFromHdfs(getSchema(), true);
    }

    @Test
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenOutputDirectoryContainsFiles() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        try {
            addElementsFromHdfs();
            fail("Exception expected");
        } catch (final Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Output directory exists and is not empty: " + outputDir));
        }
    }

    @Test
    public void shouldAddElementsWhenOutputDirectoryDoesNotExist() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir));
        }

        addElementsFromHdfs();
    }

    @Test
    public void shouldAddMultipleInputPathsFromHdfs() throws Exception {
        // Given
        String inputDir2 = testFolder.getRoot().getAbsolutePath() + "/inputDir2";
        String inputDir3 = testFolder.getRoot().getAbsolutePath() + "/inputDir3";
        final Map<String, String> inputMappers = new HashMap<>();
        inputMappers.put(new Path(inputDir).toString(), TextMapperGeneratorImpl.class.getName());
        inputMappers.put(new Path(inputDir2).toString(), TextMapperGeneratorImpl.class.getName());

        createInputFile(inputDir, 0, NUM_ELEMENTS);
        createInputFile(inputDir2, NUM_ELEMENTS, 2 * NUM_ELEMENTS);
        createInputFile(inputDir3, 2 * NUM_ELEMENTS, 3 * NUM_ELEMENTS);

        final Graph graph = createGraph(getSchema());

        // When
        graph.execute(createOperation(inputDir3, inputMappers).build(), new User());

        // Then
        final CloseableIterable<? extends Element> elements = graph.execute(
                new GetAllElements(),
                new User.Builder()
                        .dataAuth("public")
                        .dataAuth("private")
                        .build());
        final List<Element> expectedElements = new ArrayList<>(NUM_ELEMENTS);
        for (int i = 0; i < 3 * NUM_ELEMENTS; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(VERTEX_ID_PREFIX + i)
                    .property(TestPropertyNames.COUNT, 2 * DUPLICATES)
                    .property(TestPropertyNames.VISIBILITY, "private")
                    .property(TestPropertyNames.TIMESTAMP, 2L)
                    .build();
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(VERTEX_ID_PREFIX + i)
                    .dest(VERTEX_ID_PREFIX + (i + 1))
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 2 * DUPLICATES)
                    .property(TestPropertyNames.VISIBILITY, "public")
                    .property(TestPropertyNames.TIMESTAMP, 2L)
                    .build();
            expectedElements.add(entity);
            expectedElements.add(edge);
        }
        ElementUtil.assertElementEquals(expectedElements, elements);
    }

    protected AddElementsFromHdfs.Builder createOperation(final String inputDirParam, final Map<String, String> inputMappers) {
        return new AddElementsFromHdfs.Builder()
                .inputMapperPairs(inputMappers)
                .addInputMapperPair(new Path(inputDirParam).toString(), TextMapperGeneratorImpl.class.getName())
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .useProvidedSplits(false)
                .splitsFilePath(splitsFile)
                .workingPath(workingDir);
    }

    private AddElementsFromHdfs.Builder createOperation() {
        return createOperation(inputDir, null);
    }

    protected void addElementsFromHdfs() throws Exception {
        addElementsFromHdfs(getSchema(), true);
    }

    protected void addElementsFromHdfs(final Schema schema, final boolean fullyAggregated) throws Exception {
        // Given
        createInputFile(inputDir, 0, NUM_ELEMENTS);
        final Graph graph = createGraph(schema);

        // When
        graph.execute(createOperation().build(), new User());

        // Then
        final CloseableIterable<? extends Element> allElements =
                graph.execute(
                        new GetAllElements(),
                        new User.Builder()
                                .dataAuth("public")
                                .dataAuth("private")
                                .build()
                );

        final CloseableIterable<? extends Element> publicElements =
                graph.execute(
                        new GetAllElements(),
                        new User.Builder()
                                .dataAuth("public")
                                .build()
                );

        final CloseableIterable<? extends Element> privateElements =
                graph.execute(
                        new GetAllElements(),
                        new User.Builder()
                                .dataAuth("private")
                                .build()
                );

        final CloseableIterable<? extends Element> noElements = graph.execute(new GetAllElements(), new User());

        final List<Element> expectedEdges = new ArrayList<>();
        final List<Element> expectedEntities = new ArrayList<>();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(VERTEX_ID_PREFIX + i)
                    .property(TestPropertyNames.COUNT, 2 * DUPLICATES)
                    .property(TestPropertyNames.VISIBILITY, "private")
                    .property(TestPropertyNames.TIMESTAMP, 2L)
                    .build();
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(VERTEX_ID_PREFIX + i)
                    .dest(VERTEX_ID_PREFIX + (i + 1))
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 2 * DUPLICATES)
                    .property(TestPropertyNames.VISIBILITY, "public")
                    .property(TestPropertyNames.TIMESTAMP, 2L)
                    .build();
            if (fullyAggregated) {
                expectedEntities.add(entity);
                expectedEdges.add(edge);
            } else {
                // Entities were aggregated
                expectedEntities.add(entity);

                // Edges were not aggregated
                edge.putProperty(TestPropertyNames.COUNT, 1);
                edge.putProperty(TestPropertyNames.TIMESTAMP, 1L);
                for (int j = 0; j < DUPLICATES; j++) {
                    expectedEdges.add(edge);
                }

                final Edge edge2 = new Edge.Builder()
                        .group(edge.getGroup())
                        .source(edge.getSource())
                        .dest(edge.getDestination())
                        .directed(edge.isDirected())
                        .property(TestPropertyNames.VISIBILITY, "public")
                        .property(TestPropertyNames.COUNT, 1)
                        .property(TestPropertyNames.TIMESTAMP, 2L)
                        .build();
                for (int j = 0; j < DUPLICATES; j++) {
                    expectedEdges.add(edge2);
                }
            }
        }

        ElementUtil.assertElementEquals(new ChainedIterable<>(expectedEntities, expectedEdges), allElements);
        ElementUtil.assertElementEquals(expectedEdges, publicElements);
        ElementUtil.assertElementEquals(expectedEntities, privateElements);
        ElementUtil.assertElementEquals(Collections.emptyList(), noElements);
    }

    private void createInputFile(final String inputDir, final int start, final int end) throws IOException, StoreException {
        final Path inputPath = new Path(inputDir);
        final Path inputFilePath = new Path(inputDir + "/file.txt");
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(inputPath);

        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath, true)))) {
            // Add backwards to ensure the store is capable of ordering the elements if required
            for (int i = (end - 1); i >= start; i--) {
                for (int duplicates = 0; duplicates < DUPLICATES; duplicates++) {
                    writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + ",1\n");
                    writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + ",2\n");
                    writer.write(TestGroups.EDGE + "," + VERTEX_ID_PREFIX + i + "," + VERTEX_ID_PREFIX + (i + 1) + ",1\n");
                    writer.write(TestGroups.EDGE + "," + VERTEX_ID_PREFIX + i + "," + VERTEX_ID_PREFIX + (i + 1) + ",2\n");
                }
            }
        }
    }

    protected JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        conf.set(JTConfig.JT_IPC_ADDRESS, JTConfig.LOCAL_FRAMEWORK_NAME);

        return conf;
    }

    private Schema getSchema() {
        return new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_TRUE, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .type(TestTypes.VISIBILITY, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringDeduplicateConcat())
                        .build())
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .visibilityProperty(TestPropertyNames.VISIBILITY)
                .build();
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
            if (3 == parts.length) {
                return new Entity.Builder()
                        .group(parts[0])
                        .vertex(parts[1])
                        .property(TestPropertyNames.COUNT, 1)
                        .property(TestPropertyNames.VISIBILITY, "private")
                        .property(TestPropertyNames.TIMESTAMP, Long.parseLong(parts[2]))
                        .build();
            }

            return new Edge.Builder()
                    .group(parts[0])
                    .source(parts[1])
                    .dest(parts[2])
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 1)
                    .property(TestPropertyNames.VISIBILITY, "public")
                    .property(TestPropertyNames.TIMESTAMP, Long.parseLong(parts[3]))
                    .build();
        }
    }
}
