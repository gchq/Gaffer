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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.AUTHS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.CUSTOM_PROPERTIES_AUTHS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.FILE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.GAFFER_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.SCHEMA;

public class FederatedStoreProperties extends StoreProperties {

    public FederatedStoreProperties() {
        setStoreClass(FederatedStore.class);
    }

    public void setGraphIds(final String graphIdsCSV) {
        set(KEY_GRAPH_IDS, graphIdsCSV);
    }

    public void setTrueSkipFailedExecution() {
        setSkipFailedExecution(true);
    }

    public void setFalseSkipFailedExecution() {
        setSkipFailedExecution(false);
    }

    public void setCustomPropertyAuths(final String auths) {
        set(getCustomPropsKey(), auths);
    }

    public static String getCustomPropsKey() {
        return String.format("%s.%s", GAFFER_FEDERATED_STORE, CUSTOM_PROPERTIES_AUTHS);
    }

    public void setGraphAuth(final String graphId, final String authCSV) {
        set(getGraphAuthsKey(graphId), authCSV);
    }

    public void setSkipFailedExecution(final boolean b) {
        set(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE, Boolean.toString(b));
    }

    public void setGraphPropFile(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.properties, LocationEnum.file);
        set(key, file);
    }

    public void setGraphSchemaFile(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.schema, LocationEnum.file);
        set(key, file);
    }

    public void setGraphPropId(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.properties, LocationEnum.id);
        set(key, file);
    }

    public void setGraphSchemaId(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.schema, LocationEnum.id);
        set(key, file);
    }

    public static String getGraphConfigKey(final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum locationEnum) {
        return String.format("%s.%s.%s.%s", GAFFER_FEDERATED_STORE, graphId, graphConfigEnum.value, locationEnum.value);
    }


    public static String getGraphAuthsKey(final String graphId) {
        return String.format("%s.%s.%s", GAFFER_FEDERATED_STORE, graphId, AUTHS);
    }

    public void setCacheProperties(final String cacheServiceClassString) {
        set(CacheProperties.CACHE_SERVICE_CLASS, cacheServiceClassString);
    }

    public enum GraphConfigEnum {
        schema(SCHEMA), properties(PROPERTIES);

        private final String value;

        GraphConfigEnum(final String value) {
            this.value = value;
        }
    }

    public enum LocationEnum {
        file(FILE), id(FederatedStoreConstants.ID);

        private final String value;

        LocationEnum(final String value) {
            this.value = value;
        }
    }

    public static String getValueOf(final StoreProperties properties, final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum location) {
        final String key = getGraphConfigKey(graphId, graphConfigEnum, location);
        return properties.get(key);
    }

    public static String getCustomPropsValue(final StoreProperties properties) {
        return properties.get(getCustomPropsKey());
    }

    public static String getGraphAuthsValue(final StoreProperties properties, final String graphId) {
        return properties.get(getGraphAuthsKey(graphId));
    }

    public static String getGraphIdsValue(final StoreProperties properties) {
        return properties.get(KEY_GRAPH_IDS);
    }
}
