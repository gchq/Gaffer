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

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
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
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CreateSplitPointsIT {
    private static final String VERTEX_ID_PREFIX = "vertexId";
    public static final int NUM_ENTITIES = 100;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private String inputDir;
    private String outputDir;
    public String splitsDir;
    public String splitsFile;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "/inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
    }

    @Test
    public void shouldAddElementsFromHdfs() throws Exception {
        // Given
        createInputFile();

        final SingleUseMockAccumuloStoreWithTabletServers store = new SingleUseMockAccumuloStoreWithTabletServers();
        store.initialise(
                "graphId1",
                Schema.fromJson(StreamUtil.schemas(getClass())),
                AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass()))
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
                .then(new SplitStore.Builder()
                        .inputPath(splitsFile)
                        .build())
                .build(), new User());

        // Then
        final List<Text> splitsOnTable = Lists.newArrayList(store.getConnection().tableOperations().listSplits(store.getTableName(), 10));
        final List<String> stringSplitsOnTable = Lists.transform(splitsOnTable, t -> StringUtil.toString(t.getBytes()));
        final List<String> fileSplits = FileUtils.readLines(new File(splitsFile));
        final List<String> fileSplitsDecoded = Lists.transform(fileSplits, t -> StringUtil.toString(Base64.decodeBase64(t)));
        assertEquals(fileSplitsDecoded, stringSplitsOnTable);
        assertEquals(2, splitsOnTable.size());
        assertEquals(VERTEX_ID_PREFIX + "53\u0000\u0001", stringSplitsOnTable.get(0));
        assertEquals(VERTEX_ID_PREFIX + "99\u0000\u0001", stringSplitsOnTable.get(1));
    }

    private void createInputFile() throws IOException, StoreException {
        final Path inputPath = new Path(inputDir);
        final Path inputFilePath = new Path(inputDir + "/file.txt");
        final FileSystem fs = FileSystem.getLocal(createLocalConf());
        fs.mkdirs(inputPath);

        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(inputFilePath, true)))) {
            for (int i = 0; i < NUM_ENTITIES; i++) {
                writer.write(TestGroups.ENTITY + "," + VERTEX_ID_PREFIX + i + "\n");
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

    private static final class SingleUseMockAccumuloStoreWithTabletServers extends SingleUseMockAccumuloStore {
        @Override
        public List<String> getTabletServers() throws StoreException {
            return Arrays.asList("1", "2", "3");
        }
    }
}
