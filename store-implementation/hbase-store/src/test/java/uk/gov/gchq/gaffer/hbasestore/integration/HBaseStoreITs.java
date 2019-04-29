/*
 * Copyright 2016-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.integration;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.SingleUseMiniHBaseStore;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderIT;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.integration.impl.loader.AddElementsLoaderIT;
import uk.gov.gchq.gaffer.store.StoreException;

public class HBaseStoreITs extends AbstractStoreITs {
    private static final HBaseProperties STORE_PROPERTIES = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(HBaseStoreITs.class));

    public HBaseStoreITs() {
        this(STORE_PROPERTIES);
        try {
            TableUtils.dropAllTables(new SingleUseMiniHBaseStore().getConnection());
        } catch (final StoreException e) {
            // ignore any errors that occur when dropping test tables
        }
    }

    protected HBaseStoreITs(final HBaseProperties storeProperties) {
        super(storeProperties);
        addExtraTest(AddElementsFromHdfsLoaderIT.class);
        skipTestMethod(AddElementsLoaderIT.class, "shouldGetElementsWithMatchedVertex", "known issue with INGEST/QUERY AGGREGATION");
        skipTestMethod(AddElementsFromHdfsLoaderIT.class, "shouldAddElementsFromHdfsWhenDirectoriesAlreadyExist", "Known issue that directory is not empty");
        skipTestMethod(AddElementsFromHdfsLoaderIT.class, "shouldThrowExceptionWhenAddElementsFromHdfsWhenFailureDirectoryContainsFiles", "known issue that directory is not empty");
    }
}
