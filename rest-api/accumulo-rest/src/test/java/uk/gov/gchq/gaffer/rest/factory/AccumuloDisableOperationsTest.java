/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.GenerateSplitPointsFromSample;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.rest.DisableOperationsTest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class AccumuloDisableOperationsTest extends DisableOperationsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloDisableOperationsTest.class);
    private static final String STORE_PROPS_PATH = "src/test/resources/store.properties";

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(STORE_PROPS_PATH);
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @BeforeAll
    public static void setUpStore() {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @BeforeEach
    public void resetPropertiesIntoTheTempFile() throws IOException {
        before();
        createUpdatedPropertiesFile(PROPERTIES, this.storePropsPath.getAbsolutePath());
    }

    private void createUpdatedPropertiesFile(AccumuloProperties accumuloProperties, String filename) {
        Properties properties = accumuloProperties.getProperties();
        try {
            OutputStream fos = new FileOutputStream(filename);
            properties.store(fos, "AccumuloDisableOperationsTest - " + filename + " with current zookeeper");
            fos.close();
        } catch (IOException e) {
            LOGGER.error("Failed to write Properties file: " + filename + ": " + e.getMessage());
        }
    }

    @Override
    protected Class<? extends Operation>[] getDisabledOperations() {
        return new Class[] {SplitStore.class,
                GenerateSplitPointsFromSample.class,
                AddElementsFromHdfs.class,
                SampleDataForSplitPoints.class,
                ImportAccumuloKeyValueFiles.class};
    }
}
