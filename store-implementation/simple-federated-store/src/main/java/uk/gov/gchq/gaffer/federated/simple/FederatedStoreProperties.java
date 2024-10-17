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

import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.InputStream;
import java.nio.file.Path;

public class FederatedStoreProperties extends StoreProperties {
    /**
     * Property key for federated stores default graph IDs
     */
    public static final String PROP_DEFAULT_GRAPH_IDS = "gaffer.store.federated.default.graphIds";
    /**
     * Property key for setting if the default is to apply element aggregation or not
     */
    public static final String PROP_DEFAULT_MERGE_ELEMENTS = "gaffer.store.federated.default.aggregateElements";
    /**
     * Property key for setting if public graphs can be added to the store or not
     */
    public static final String PROP_ALLOW_PUBLIC_GRAPHS = "gaffer.store.federated.allowPublicGraphs";
    /**
     * Property key for setting a custom name for the graph cache, by default
     * this will be "federatedGraphCache_" followed by the federated graph ID.
     */
    public static final String PROP_GRAPH_CACHE_NAME = "gaffer.store.federated.graphCache.name";
    /**
     * Property key for the class to use when merging number results
     */
    public static final String PROP_MERGE_CLASS_NUMBER = "gaffer.store.federated.merge.number.class";
    /**
     * Property key for the class to use when merging string results
     */
    public static final String PROP_MERGE_CLASS_STRING = "gaffer.store.federated.merge.string.class";
    /**
     * Property key for the class to use when merging boolean results
     */
    public static final String PROP_MERGE_CLASS_BOOLEAN = "gaffer.store.federated.merge.boolean.class";
    /**
     * Property key for the class to use when merging collection results
     */
    public static final String PROP_MERGE_CLASS_COLLECTION = "gaffer.store.federated.merge.collection.class";
    /**
     * Property key for the class to use when merging values of a Map result
     */
    public static final String PROP_MERGE_CLASS_MAP = "gaffer.store.federated.merge.map.class";
    /**
     * Property key for the class to use when merging elements
     */
    public static final String PROP_MERGE_CLASS_ELEMENTS = "gaffer.store.federated.merge.elements.class";

    public FederatedStoreProperties() {
        super(FederatedStore.class);
    }

    public FederatedStoreProperties(final Path propFileLocation) {
        super(propFileLocation, FederatedStore.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, FederatedStoreProperties.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, FederatedStoreProperties.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, FederatedStoreProperties.class);
    }
}
