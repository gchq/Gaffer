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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloTestClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.ExecutorService;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.exception.StorageException;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;

public class PredefinedFederatedStore extends FederatedStore {
    public static final String ACCUMULO_GRAPH_WITH_EDGES = "AccumuloStoreContainingEdges";
    public static final String ACCUMULO_GRAPH_WITH_ENTITIES = "AccumuloStoreContainingEntities";
    public static final String ALL_GRAPH_IDS = ACCUMULO_GRAPH_WITH_EDGES + "," + ACCUMULO_GRAPH_WITH_ENTITIES;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/singleUseAccumuloStore.properties"));
    private static AccumuloTestClusterManager accumuloTestClusterManager = null;

    static {
        accumuloTestClusterManager = new AccumuloTestClusterManager(PROPERTIES, CommonTestConstants.TMP_DIRECTORY.getAbsolutePath());
    }

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
        ExecutorService.shutdown();

        super.initialise(graphId, schema, properties);

        // Accumulo store just contains edges
        try {
            addGraphs(null, User.UNKNOWN_USER_ID, false, new GraphSerialisable.Builder()
                    .config(new GraphConfig(ACCUMULO_GRAPH_WITH_EDGES))
                    .schema(new Schema.Builder()
                            .merge(schema.clone())
                            .entities(Collections.emptyMap())
                            .build())
                    .properties(PROPERTIES)
                    .build());

            // Accumulo store just contains entities
            addGraphs(null, User.UNKNOWN_USER_ID, false, new GraphSerialisable.Builder()
                    .config(new GraphConfig(ACCUMULO_GRAPH_WITH_ENTITIES))
                    .schema(new Schema.Builder()
                            .merge(schema.clone())
                            .edges(Collections.emptyMap())
                            .build())
                    .properties(PROPERTIES)
                    .build());
        } catch (final StorageException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }
}
