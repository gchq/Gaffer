/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.ExecutorService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.schema.Schema;
import uk.gov.gchq.gaffer.graph.util.GraphConfig;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.library.HashMapLibrary;
import uk.gov.gchq.gaffer.store.util.Config;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;

public class PredefinedFederatedStore extends FederatedStore {
    public static final String ACCUMULO_GRAPH_WITH_EDGES = "AccumuloStoreContainingEdges";
    public static final String ACCUMULO_GRAPH_WITH_ENTITIES = "AccumuloStoreContainingEntities";
    public static final String ALL_GRAPH_IDS = ACCUMULO_GRAPH_WITH_EDGES + "," + ACCUMULO_GRAPH_WITH_ENTITIES;

    @Override
    public void initialise(final Config config) throws StoreException {
        HashMapLibrary.clear();
        CacheServiceLoader.shutdown();
        ExecutorService.shutdown();

        super.initialise(config);

        // Accumulo store just contains edges
        try {
            addStores(null, User.UNKNOWN_USER_ID, false,
                    Store.createStore(new GraphConfig.Builder()
                            .id(ACCUMULO_GRAPH_WITH_EDGES)
                            .schema(new Schema.Builder()
                                    .merge(((GraphConfig) config).getSchema().clone())
                                    .entities(Collections.emptyMap())
                                    .build())
                            .storeProperties(StreamUtil.openStream(getClass(),
                                    "properties/singleUseMockAccStore.properties"))
                            .build()));

            // Accumulo store just contains entities
            addStores(null, User.UNKNOWN_USER_ID, false,
                    Store.createStore(new GraphConfig.Builder()
                            .id(ACCUMULO_GRAPH_WITH_ENTITIES)
                            .schema(new Schema.Builder()
                                    .merge(((GraphConfig) config).getSchema().clone())
                                    .edges(Collections.emptyMap())
                                    .build())
                            .storeProperties(StreamUtil.openStream(getClass(),
                                    "properties/singleUseMockAccStore.properties"))
                            .build()));
        } catch (final StorageException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }
}
