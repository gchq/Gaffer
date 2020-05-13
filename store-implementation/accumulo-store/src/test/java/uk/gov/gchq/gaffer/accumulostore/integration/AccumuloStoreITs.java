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
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderIT;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

public class AccumuloStoreITs extends AbstractStoreITs {
    private static final AccumuloProperties BYTE_STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));
    private static Store store;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUpDatabase(BYTE_STORE_PROPERTIES);
    }

    public static void setUpDatabase(AccumuloProperties inStoreProperties) throws Exception {
        // Get the store class from the properties supplied
        Class currentClass = new Object() { }.getClass().getEnclosingClass();
        final String storeClass = inStoreProperties.getStoreClass();
        if (null == storeClass) {
            throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_CLASS);
        }
        // Instantiate the store class
        try {
            store = Class.forName(storeClass)
                    .asSubclass(Store.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass, e);
        }
        // Set up the data store and set the properties to suit.
        AccumuloProperties accumuloProperties = (AccumuloProperties) store.setUpTestDB(inStoreProperties);
        if (null != accumuloProperties.getInstance()) {
            inStoreProperties.setInstance(accumuloProperties.getInstance());
        }
        if (null != accumuloProperties.getZookeepers()) {
            inStoreProperties.setZookeepers(accumuloProperties.getZookeepers());
        }
        if (null != accumuloProperties.getNamespace()) {
            inStoreProperties.setNamespace(accumuloProperties.getNamespace());
        }
    }

    @AfterClass
    public static void tearDown() {
        store.tearDownTestDB();
    }

    public AccumuloStoreITs() {
        this(BYTE_STORE_PROPERTIES);
    }

    protected AccumuloStoreITs(final AccumuloProperties storeProperties) {
        super(storeProperties);
        addExtraTest(AddElementsFromHdfsLoaderIT.class);
    }
}
