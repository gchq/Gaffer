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

import org.junit.AfterClass;
import org.junit.BeforeClass;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloTestClusterManager;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderIT;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;

public class AccumuloStoreITs extends AbstractStoreITs {
    private static final AccumuloProperties STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));
    protected static AccumuloTestClusterManager accumuloTestClusterManager = null;

    public AccumuloStoreITs() {
        this(STORE_PROPERTIES);
    }

    protected AccumuloStoreITs(final AccumuloProperties storeProperties) {
        super(storeProperties);
        addExtraTest(AddElementsFromHdfsLoaderIT.class);
    }

    @BeforeClass
    public static void setUpStore() {
        accumuloTestClusterManager = new AccumuloTestClusterManager(STORE_PROPERTIES);
    }

    @AfterClass
    public static void tearDownStore() {
        accumuloTestClusterManager.close();
    }

}
