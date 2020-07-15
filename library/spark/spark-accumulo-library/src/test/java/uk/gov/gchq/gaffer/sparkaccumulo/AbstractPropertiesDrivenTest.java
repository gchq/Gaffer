/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.sparkaccumulo;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;

import java.io.File;
import java.io.IOException;

public abstract class AbstractPropertiesDrivenTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPropertiesDrivenTest.class);
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    public static void setUpBeforeClass(String propertiesID, TemporaryFolder storeBaseFolder) {
        Class currentClass = new Object() { }.getClass().getEnclosingClass();
        AccumuloProperties suppliedProperties = AccumuloProperties
                .loadStoreProperties(currentClass.getResourceAsStream(propertiesID));
        File storeFolder = null;
        try {
            storeFolder = storeBaseFolder.newFolder();
        } catch (IOException e) {
            LOGGER.error("Failed to create sub folder in : " + storeBaseFolder.getRoot().getAbsolutePath() + ": " + e.getMessage());
        }
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(suppliedProperties, storeFolder.getAbsolutePath());
    }

    public AccumuloProperties getStoreProperties() {
        return miniAccumuloClusterManager.getStoreProperties();
    }

    public static void tearDownAfterClass() {
        miniAccumuloClusterManager.close();
    }

}
