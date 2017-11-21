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

package uk.gov.gchq.gaffer.accumulostore.integration;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AddElementsFromHdfsIT {
    private static final String VERTEX_ID_PREFIX = "vertexId";
    private static final int NUM_ELEMENTS = 1000;
    private static final List<String> TABLET_SERVERS = Arrays.asList("1", "2", "3", "4");

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private String inputDir;
    private String outputDir;
    private String failureDir;
    private String splitsDir;
    private String splitsFile;
    private String workingDir;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "/inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        failureDir = testFolder.getRoot().getAbsolutePath() + "/failureDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
        workingDir = testFolder.getRoot().getAbsolutePath() + "/workingDir";
    }

    @Test
    public void shouldAddElementsFromHdfs() throws Exception {
        addElementsFromHdfs(ByteEntityKeyPackage.class);
        addElementsFromHdfs(ClassicKeyPackage.class);
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
        addElementsFromHdfs(createStore(ByteEntityKeyPackage.class, schema), TABLET_SERVERS.size() - 1, false);
        addElementsFromHdfs(createStore(ClassicKeyPackage.class, schema), TABLET_SERVERS.size() - 1, false);
    }

    @Test
    public void shouldAddElementsFromHdfsWhenOutputDirectoryAlreadyExists() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));

        addElementsFromHdfs(ByteEntityKeyPackage.class);
        addElementsFromHdfs(ClassicKeyPackage.class);
    }

    @Test
    public void shouldAddElementsFromHdfsWhenFailureDirectoryAlreadyExists() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(failureDir));

        addElementsFromHdfs(ByteEntityKeyPackage.class);
        addElementsFromHdfs(ClassicKeyPackage.class);
    }

    @Test
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenOutputDirectoryContainsFiles() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(outputDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        try {
            addElementsFromHdfs(ByteEntityKeyPackage.class);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Output directory exists and is not empty: " + outputDir, e.getMessage());
        }

        try {
            addElementsFromHdfs(ClassicKeyPackage.class);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Output directory exists and is not empty: " + outputDir, e.getMessage());
        }
    }

    @Test
    public void shouldAddElementsWhenOutputDirectoryDoesNotExist() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir));
        }

        try {
            addElementsFromHdfs(ByteEntityKeyPackage.class);
        } catch (final Exception e) {
            fail("Error occured: " + e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddElementsFromHdfsWhenFailureDirectoryContainsFiles() throws Exception {
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(new Path(failureDir));
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(failureDir + "/someFile.txt"), true)))) {
            writer.write("Some content");
        }

        try {
            addElementsFromHdfs(ByteEntityKeyPackage.class);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertEquals("Failure directory is not empty: " + failureDir, e.getCause().getMessage());
        }

        //Previous job will output data successfully to the output dir but not load it.
        fs.delete(new Path(outputDir), true);

        try {
            addElementsFromHdfs(ClassicKeyPackage.class);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertEquals("Failure directory is not empty: " + failureDir, e.getCause().getMessage());
        }
    }

    @Test
    public void shouldNotSampleAndSplitBeforeAddingElements() throws Exception {
        shouldNotSampleAndSplitBeforeAddingElements(ByteEntityKeyPackage.class);
        shouldNotSampleAndSplitBeforeAddingElements(ClassicKeyPackage.class);
    }

    @Test
    public void shouldAddMultipleInputPathsFromHdfs() throws Exception {
        // Given
        String inputDir2 = testFolder.getRoot().getAbsolutePath() + "/inputDir2";
        String inputDir3 = testFolder.getRoot().getAbsolutePath() + "/inputDir3";
        final Map<String, String> inputMappers = new HashMap<>();
        inputMappers.put(new Path(inputDir).toString(), TextMapperGeneratorImpl.class.getName());
        inputMappers.put(new Path(inputDir2).toString(), TextMapperGeneratorImpl.class.getName());

        createInputFile(inputDir, 0, 1000);
        createInputFile(inputDir2, 1000, 2000);
        createInputFile(inputDir3, 2000, 3000);

        final Graph graph = new Graph.Builder()
                .store(createStore(ClassicKeyPackage.class))
                .build();

        // When
        graph.execute(new AddElementsFromHdfs.Builder()
                .inputMapperPairs(inputMappers)
                .addInputMapperPair(new Path(inputDir3).toString(), TextMapperGeneratorImpl.class.getName())
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .useProvidedSplits(false)
                .splitsFilePath(splitsFile)
                .workingPath(workingDir)
                .build(), new User());

        // Then
        final CloseableIterable<? extends Element> elements = graph.execute(new GetAllElements(), new User());
        final List<Element> expectedElements = new ArrayList<>(NUM_ELEMENTS);
        for (int i = 0; i < 3000; i++) {
            expectedElements.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(VERTEX_ID_PREFIX + i)
                    .property(TestPropertyNames.COUNT, 2)
                    .build());
            expectedElements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(VERTEX_ID_PREFIX + i)
                    .dest(VERTEX_ID_PREFIX + (i + 1))
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 2)
                    .build());
        }
        ElementUtil.assertElementEquals(expectedElements, elements);
    }

    private void shouldNotSampleAndSplitBeforeAddingElements(final Class<? extends AccumuloKeyPackage> keyPackage) throws Exception {
        final AccumuloStore store = createStore(keyPackage);

        // Add some splits so that the sample and split operations do not get invoked.
        final SortedSet<Text> splits = Sets.newTreeSet(Arrays.asList(new Text("1"), new Text("2")));
        store.getConnection().tableOperations().addSplits(store.getTableName(), splits);
        assertEquals(splits.size(), store.getConnection().tableOperations().listSplits(store.getTableName()).size());

        addElementsFromHdfs(store, splits.size());
    }

    private void addElementsFromHdfs(final Class<? extends AccumuloKeyPackage> keyPackageClass)
            throws Exception {
        addElementsFromHdfs(createStore(keyPackageClass), TABLET_SERVERS.size() - 1);
    }

    private void addElementsFromHdfs(final AccumuloStore store, final int expectedSplits) throws Exception {
        addElementsFromHdfs(store, expectedSplits, true);
    }

    private void addElementsFromHdfs(final AccumuloStore store, final int expectedSplits, final boolean fullyAggregated) throws Exception {
        // Given
        createInputFile(inputDir, 0, 1000);
        final Graph graph = new Graph.Builder()
                .store(store)
                .build();

        // When
        graph.execute(new AddElementsFromHdfs.Builder()
                .addInputMapperPair(new Path(inputDir).toString(), TextMapperGeneratorImpl.class.getName())
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .useProvidedSplits(false)
                .splitsFilePath(splitsFile)
                .workingPath(workingDir)
                .build(), new User());

        // Then
        final CloseableIterable<? extends Element> elements = graph.execute(new GetAllElements(), new User());
        final List<Element> expectedElements = new ArrayList<>(NUM_ELEMENTS);
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            expectedElements.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(VERTEX_ID_PREFIX + i)
                    .property(TestPropertyNames.COUNT, 2)
                    .build());
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(VERTEX_ID_PREFIX + i)
                    .dest(VERTEX_ID_PREFIX + (i + 1))
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 2)
                    .build();
            if (fullyAggregated) {
                expectedElements.add(edge);
            } else {
                edge.putProperty(TestPropertyNames.COUNT, 1);
                expectedElements.add(edge);
                expectedElements.add(edge);
            }
        }
        ElementUtil.assertElementEquals(expectedElements, elements);
        assertEquals(expectedSplits, store.getConnection().tableOperations().listSplits(store.getTableName()).size());
    }

    private void createInputFile(final String inputDir, final int start, final int end) throws IOException, StoreException {
        final Path inputPath = new Path(inputDir);
        final Path inputFilePath = new Path(inputDir + "/file.txt");
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(inputPath);

        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath, true)))) {
            for (int i = start; i < end; i++) {
                writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
                writer.write(TestGroups.EDGE + "," + VERTEX_ID_PREFIX + i + "," + VERTEX_ID_PREFIX + (i + 1) + "\n");

                // Add deduplicates
                writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
                writer.write(TestGroups.EDGE + "," + VERTEX_ID_PREFIX + i + "," + VERTEX_ID_PREFIX + (i + 1) + "\n");
            }
        }
    }

    private JobConf createLocalConf() {
        // Set up local conf
        final JobConf conf = new JobConf();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.jobtracker.address", "local");

        return conf;
    }

    private AccumuloStore createStore(final Class<? extends AccumuloKeyPackage> keyPackageClass) throws Exception {
        return createStore(keyPackageClass, getSchema());
    }

    private Schema getSchema() {
        return Schema.fromJson(StreamUtil.schemas(getClass()));
    }

    private AccumuloStore createStore(final Class<? extends AccumuloKeyPackage> keyPackageClass, final Schema schema) throws Exception {
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        properties.setKeyPackageClass(keyPackageClass.getName());
        properties.setInstance("instance_" + keyPackageClass.getName());

        final AccumuloStore store = new SingleUseMockAccumuloStoreWithTabletServers();
        store.initialise(keyPackageClass.getSimpleName() + "Graph", schema, properties);
        assertEquals(0, store.getConnection().tableOperations().listSplits(store.getTableName()).size());
        return store;
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

    private static final class SingleUseMockAccumuloStoreWithTabletServers extends SingleUseMockAccumuloStore {
        @Override
        public List<String> getTabletServers() throws StoreException {
            return TABLET_SERVERS;
        }
    }
}
