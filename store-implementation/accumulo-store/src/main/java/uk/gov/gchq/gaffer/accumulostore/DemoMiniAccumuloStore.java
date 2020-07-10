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

package uk.gov.gchq.gaffer.accumulostore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;

public class DemoMiniAccumuloStore extends AccumuloStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoMiniAccumuloStore.class);
    private static AccumuloTestClusterManager accumuloTestClusterManager;
    private static final AccumuloProperties ACCUMULO_PROPERTIES = new AccumuloProperties();

    static {
        ACCUMULO_PROPERTIES.setStoreClass(MiniAccumuloStore.class);
        ACCUMULO_PROPERTIES.setZookeepers("aZookeeper");
        ACCUMULO_PROPERTIES.setUser("user01");
        ACCUMULO_PROPERTIES.setPassword("password01");
        ACCUMULO_PROPERTIES.setInstance("instance01");

        final String tmpDirectoryProperty = System.getProperty("java.io.tmpdir");
        File storeFolder = null;
        if (null != tmpDirectoryProperty) {
            storeFolder = new File(tmpDirectoryProperty);
        } else {
            LOGGER.error(DemoMiniAccumuloStore.class + ": Could not create storeFolder directory");
        }

        accumuloTestClusterManager = new AccumuloTestClusterManager(ACCUMULO_PROPERTIES, storeFolder.getAbsolutePath());
    }

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, schema, ACCUMULO_PROPERTIES);
    }

}
