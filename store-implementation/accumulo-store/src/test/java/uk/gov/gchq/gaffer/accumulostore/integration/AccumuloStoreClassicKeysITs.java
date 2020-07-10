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
package uk.gov.gchq.gaffer.accumulostore.integration;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloTestClusterManager;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;

import java.io.File;
import java.io.IOException;

public class AccumuloStoreClassicKeysITs extends AccumuloStoreITs {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStoreClassicKeysITs.class);
    private static final AccumuloProperties STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloStoreClassicKeysITs.class, "/accumuloStoreClassicKeys.properties"));

    @ClassRule
    public static TemporaryFolder storeBaseFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setUpStore() {
        File storeFolder = null;
        try {
            storeFolder = storeBaseFolder.newFolder();
        } catch (IOException e) {
            LOGGER.error("Failed to create sub folder in : " + storeBaseFolder.getRoot().getAbsolutePath() + ": " + e.getMessage());
        }
        accumuloTestClusterManager = new AccumuloTestClusterManager(STORE_PROPERTIES, storeFolder.getAbsolutePath());
    }

    public AccumuloStoreClassicKeysITs() {
        super(STORE_PROPERTIES);
    }
}
