/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.hdfs.integration.loader;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.AbstractStandaloneLoaderIT;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public abstract class AddElementsFromHdfsLoaderIT extends AbstractStandaloneLoaderIT<AddElementsFromHdfs> {

    private static final String VERTEX_ID_PREFIX = "vertexId";
    public static final int NUM_ELEMENTS = 100;
    public static final int DUPLICATES = 1;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected String inputDir;
    protected String outputDir;
    protected String failureDir;
    protected String splitsDir;
    protected String splitsFile;
    protected String workingDir;
    protected String stagingDir;

    @Before
    public void setup() throws Exception {
        inputDir = testFolder.getRoot().getAbsolutePath() + "/inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        failureDir = testFolder.getRoot().getAbsolutePath() + "/failureDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
        workingDir = testFolder.getRoot().getAbsolutePath() + "/workingDir";
        stagingDir = testFolder.getRoot().getAbsolutePath() + "/stagingDir";

        super.setup();
    }

    @Override
    protected void configure(final Iterable<? extends Element> elements) throws Exception {
        createInputFile(inputDir, 0, NUM_ELEMENTS);
    }

    @Override
    protected AddElementsFromHdfs createOperation(final Iterable<? extends Element> elements) {
        return new AddElementsFromHdfs.Builder()
                .addInputMapperPair(new Path(inputDir).toString(), TextMapperGeneratorImpl.class.getName())
                .outputPath(outputDir)
                .failurePath(failureDir)
                .jobInitialiser(new TextJobInitialiser())
                .useProvidedSplits(false)
                .splitsFilePath(splitsFile)
                .workingPath(workingDir)
                .build();
    }

    @Override
    protected Map<EdgeId, Edge> createBasicSchemaEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = (NUM_ELEMENTS - 1); i >= 0; i--) {
            for (int duplicates = 0; duplicates < DUPLICATES; duplicates++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_ID_PREFIX + i)
                        .dest(VERTEX_ID_PREFIX + (i + 1))
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 2L)
                        .build();
                addToMap(edge, edges);
            }
        }
        return edges;
    }

    @Override
    protected Map<EdgeId, Edge> createFullSchemaEdges() {
        final Map<EdgeId, Edge> edges = new HashMap<>();
        for (int i = (NUM_ELEMENTS - 1); i >= 0; i--) {
            for (int duplicates = 0; duplicates < DUPLICATES; duplicates++) {
                final Edge edge = new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source(VERTEX_ID_PREFIX + i)
                        .dest(VERTEX_ID_PREFIX + (i + 1))
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 2L)
                        .build();
                addToMap(edge, edges);
            }
        }
        return edges;
    }

    @Override
    protected Map<EntityId, Entity> createBasicSchemaEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = (NUM_ELEMENTS - 1); i >= 0; i--) {
            for (int duplicates = 0; duplicates < DUPLICATES; duplicates++) {
                final Entity entity = new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(VERTEX_ID_PREFIX + i)
                        .property(TestPropertyNames.COUNT, 2L)
                        .build();
                addToMap(entity, entities);
            }
        }
        return entities;
    }

    @Override
    protected Map<EntityId, Entity> createFullSchemaEntities() {
        final Map<EntityId, Entity> entities = new HashMap<>();
        for (int i = (NUM_ELEMENTS - 1); i >= 0; i--) {
            for (int duplicates = 0; duplicates < DUPLICATES; duplicates++) {
                final Entity entity = new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex(VERTEX_ID_PREFIX + i)
                        .property(TestPropertyNames.COUNT, 2L)
                        .property(TestPropertyNames.PROP_3, "String")
                        .property(TestPropertyNames.PROP_4, new FreqMap())
                        .property(TestPropertyNames.PROP_5, new HashSet<>())
                        .property(TestPropertyNames.VISIBILITY, "all")
                        .build();
                addToMap(entity, entities);
            }
        }
        return entities;
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
                    writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
                    writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
                    writer.write(TestGroups.EDGE + "," + VERTEX_ID_PREFIX + i + "," + VERTEX_ID_PREFIX + (i + 1) + "\n");
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
                return new Edge.Builder()
                        .group(parts[0])
                        .source(parts[1])
                        .dest(parts[2])
                        .directed(true)
                        .property(TestPropertyNames.COUNT, 2L)
                        .build();
            }

            return new Entity.Builder()
                    .group(parts[0])
                    .vertex(parts[1])
                    .property(TestPropertyNames.COUNT, 2L)
                    .property(TestPropertyNames.PROP_3, "String")
                    .property(TestPropertyNames.PROP_4, new FreqMap())
                    .property(TestPropertyNames.PROP_5, new HashSet<>())
                    .property(TestPropertyNames.VISIBILITY, "all")
                    .build();
        }
    }
}
