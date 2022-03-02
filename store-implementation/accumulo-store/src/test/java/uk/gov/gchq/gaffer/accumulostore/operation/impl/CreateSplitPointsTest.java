/*
 * Copyright 2017-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.TextJobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateSplitPointsTest {
    private static final String VERTEX_ID_PREFIX = "vertexId";
    public static final int NUM_ENTITIES = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSplitPointsTest.class);

    @TempDir
    public final File testFolder = CommonTestConstants.TMP_DIRECTORY;

    private FileSystem fs;

    private String inputDir;
    private String outputDir;
    public String splitsFile;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(currentClass));

    @BeforeEach
    public void setup() throws IOException {

        fs = createFileSystem();

        final String root = fs.resolvePath(new Path("/")).toString()
                .replaceFirst("/$", "")
                + testFolder.getAbsolutePath();


        LOGGER.info("using root dir: " + root);

        inputDir = root + "/inputDir";
        outputDir = root + "/outputDir";
        splitsFile = root + "/splitsDir/splits";
    }

    @Test
    public void shouldAddElementsFromHdfs() throws Exception {
        // Given
        createInputFile();

        final SingleUseAccumuloStoreWithTabletServers store = new SingleUseAccumuloStoreWithTabletServers();
        store.initialise(
                "graphId1",
                Schema.fromJson(StreamUtil.schemas(getClass())),
                PROPERTIES
        );

        final Graph graph = new Graph.Builder()
                .store(store)
                .build();

        // When
        graph.execute(new OperationChain.Builder()
                .first(new SampleDataForSplitPoints.Builder()
                        .jobInitialiser(new TextJobInitialiser())
                        .addInputMapperPair(inputDir, TextMapperGeneratorImpl.class.getName())
                        .outputPath(outputDir)
                        .proportionToSample(1f)
                        .validate(true)
                        .mappers(5)
                        .splitsFilePath(splitsFile)
                        .compressionCodec(null)
                        .build())
                .then(new SplitStoreFromFile.Builder()
                        .inputPath(splitsFile)
                        .build())
                .build(), new User());

        // Then
        final List<Text> splitsOnTable = Lists.newArrayList(store.getConnection().tableOperations().listSplits(store.getTableName(), 10));
        final List<String> stringSplitsOnTable = Lists.transform(splitsOnTable, t -> StringUtil.toString(t.getBytes()));
        final List<String> fileSplitsDecoded = new ArrayList<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(splitsFile))));
        while (br.ready()) {
            fileSplitsDecoded.add(new String(Base64.decodeBase64(br.readLine())));
        }
        assertThat(stringSplitsOnTable).isEqualTo(fileSplitsDecoded);
        assertThat(splitsOnTable).hasSize(2);
        assertThat(stringSplitsOnTable.get(0)).isEqualTo(VERTEX_ID_PREFIX + "53\u0000\u0001");
        assertThat(stringSplitsOnTable.get(1)).isEqualTo(VERTEX_ID_PREFIX + "99\u0000\u0001");
    }

    private void createInputFile() throws IOException, StoreException {
        final Path inputPath = new Path(inputDir);
        final Path inputFilePath = new Path(inputDir + "/file.txt");

        fs.mkdirs(inputPath);

        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath, true)))) {
            for (int i = 0; i < NUM_ENTITIES; i++) {
                writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
            }
        }
    }

    private FileSystem createFileSystem() throws IOException {
        return FileSystem.get(new Configuration());
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

    private static final class SingleUseAccumuloStoreWithTabletServers extends SingleUseMiniAccumuloStore {
        @Override
        public List<String> getTabletServers() throws StoreException {
            return Arrays.asList("1", "2", "3");
        }
    }
}
