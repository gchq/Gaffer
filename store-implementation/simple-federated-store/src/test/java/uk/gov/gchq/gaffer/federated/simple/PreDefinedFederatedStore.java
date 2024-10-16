/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

/*
 * Federated Store with two graphs pre-added which is used in integration testing
 * with the AbstractStoreITs. Creates two accumulo-backed graphs and adds them to a federated
 * store.
 */
public class PreDefinedFederatedStore extends FederatedStore {
    private static final AccumuloProperties STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(PreDefinedFederatedStore.class, "single-use-accumulo-store.properties"));

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        CacheServiceLoader.shutdown();

        super.initialise(graphId, schema, properties);

        addGraph(new GraphSerialisable(new GraphConfig("graphA"),
                    schema.clone(), STORE_PROPERTIES), new GraphAccess());

        addGraph(new GraphSerialisable(new GraphConfig("graphB"),
                    schema.clone(), STORE_PROPERTIES), new GraphAccess());
    }
}
