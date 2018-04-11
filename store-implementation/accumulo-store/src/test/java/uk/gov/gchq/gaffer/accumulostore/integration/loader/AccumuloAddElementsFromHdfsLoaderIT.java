/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration.loader;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.integration.loader.AddElementsFromHdfsLoaderIT;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AccumuloAddElementsFromHdfsLoaderIT extends AddElementsFromHdfsLoaderIT {
    private static final List<String> TABLET_SERVERS = Arrays.asList("1", "2", "3", "4");

    @Override
    protected Graph createGraph(final Schema schema) throws Exception {
        final AccumuloStore store = new SingleUseMockAccumuloStoreWithTabletServers();
        store.initialise(ClassicKeyPackage.class.getSimpleName() + "Graph", schema, createStoreProperties());
        assertEquals(0, store.getConnection().tableOperations().listSplits(store.getTableName()).size());

        return new Graph.Builder()
                .store(store)
                .build();
    }

    private static final class SingleUseMockAccumuloStoreWithTabletServers extends SingleUseMockAccumuloStore {
        @Override
        public List<String> getTabletServers() throws StoreException {
            return TABLET_SERVERS;
        }
    }
}
