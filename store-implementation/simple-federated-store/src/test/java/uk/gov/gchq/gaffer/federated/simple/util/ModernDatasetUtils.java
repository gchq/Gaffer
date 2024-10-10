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

package uk.gov.gchq.gaffer.federated.simple.util;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;

public final class ModernDatasetUtils {

    private ModernDatasetUtils() {
        // utility class
    }

    public static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties
        .loadStoreProperties("/map-store.properties");
    public static final AccumuloProperties ACCUMULO_STORE_PROPERTIES = AccumuloProperties
        .loadStoreProperties("/accumulo-store.properties");

    public static Graph getBlankGraphWithModernSchema(Class<?> clazz, String graphId, StoreType storeType) {
        return new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(graphId)
                    .description("Graph using the modern dataset")
                    .build())
            .storeProperties(getStoreProperties(storeType))
            .addSchemas(StreamUtil.openStreams(clazz, "/modern/schema"))
            .build();
    }

    /**
     * Available store types for sub graphs
     */
    public enum StoreType {
        MAP, ACCUMULO;
    }

    public static StoreProperties getStoreProperties(StoreType storeType) {
        switch (storeType) {
            case MAP:
                return MAP_STORE_PROPERTIES;
            case ACCUMULO:
                return ACCUMULO_STORE_PROPERTIES;
            default:
                throw new IllegalArgumentException("Unknown StoreType: " + storeType);
        }
    }
}
