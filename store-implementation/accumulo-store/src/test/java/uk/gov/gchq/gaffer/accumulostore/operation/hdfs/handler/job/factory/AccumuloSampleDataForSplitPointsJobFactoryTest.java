/*
 * Copyright 2020 Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.hander.job.factory.AbstractJobFactoryTest;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.JobFactory;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;

public class AccumuloSampleDataForSplitPointsJobFactoryTest extends AbstractJobFactoryTest {

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloSampleDataForSplitPointsJobFactoryTest.class));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    public String inputDir;
    public String outputDir;
    public String splitsDir;
    public String splitsFile;

    @BeforeEach
    public void setup(@TempDir java.nio.file.Path tempDir) {
        inputDir = new File(tempDir.toString(), "inputDir").getAbsolutePath();
        outputDir = new File(tempDir.toString(), "outputDir").getAbsolutePath();
        splitsDir = new File(tempDir.toString(), "splitsDir").getAbsolutePath();
        splitsFile = new File(splitsDir, "splits").getAbsolutePath();
    }

    @BeforeAll
    public static void setUpStore(@TempDir java.nio.file.Path tempDir) {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @Override
    protected Store getStoreConfiguredWith(final Class<JSONSerialiser> jsonSerialiserClass, final String jsonSerialiserModules, final Boolean strictJson) throws IOException, StoreException {
        final AccumuloStore store = new SingleUseAccumuloStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloAddElementsFromHdfsJobFactoryTest.class));

        super.configureStoreProperties(PROPERTIES, jsonSerialiserClass, jsonSerialiserModules, strictJson);

        store.initialise("graphId", schema, PROPERTIES);

        final FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        fileSystem.mkdirs(new Path(outputDir));
        fileSystem.mkdirs(new Path(splitsDir));

        return store;
    }

    @Override
    protected JobFactory getJobFactory() {
        return new AccumuloSampleDataForSplitPointsJobFactory();
    }

    @Override
    protected MapReduce getMapReduceOperation() {
        return new SampleDataForSplitPoints.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, JsonMapperGenerator.class.getName())
                .build();
    }
}
