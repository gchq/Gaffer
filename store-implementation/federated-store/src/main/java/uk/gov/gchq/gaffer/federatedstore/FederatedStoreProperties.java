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

import uk.gov.gchq.gaffer.store.StoreProperties;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.PREFIX_GAFFER_FEDERATED_STORE;


/**
 * Additional {@link StoreProperties} for the {@link FederatedStore}.
 */
public class FederatedStoreProperties extends StoreProperties {
    public static final String CUSTOM_PROPERTIES_AUTHS = "customPropertiesAuths";
    public static final String KEY_GRAPH_IDS = PREFIX_GAFFER_FEDERATED_STORE + "." + "graphIds";

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

    private static String getCustomPropsKey() {
        return String.format("%s.%s", PREFIX_GAFFER_FEDERATED_STORE, CUSTOM_PROPERTIES_AUTHS);
    }

    public void setGraphAuth(final String graphId, final String authCSV) {
        set(getGraphAuthsKey(graphId), authCSV);
    }

    public void setSkipFailedExecution(final boolean b) {
        set(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE, Boolean.toString(b));
    }

    public void setGraphPropFile(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.PROPERTIES, LocationEnum.FILE);
        set(key, file);
    }

    public void setGraphSchemaFile(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.SCHEMA, LocationEnum.FILE);
        set(key, file);
    }

    public void setGraphPropId(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.PROPERTIES, LocationEnum.ID);
        set(key, file);
    }

    public void setGraphSchemaId(final String graphId, final String file) {
        final String key = getGraphConfigKey(graphId, GraphConfigEnum.SCHEMA, LocationEnum.ID);
        set(key, file);
    }

    private static String getGraphConfigKey(final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum locationEnum) {
        return String.format("%s.%s.%s.%s", PREFIX_GAFFER_FEDERATED_STORE, graphId, graphConfigEnum.value, locationEnum.value);
    }


    private static String getGraphAuthsKey(final String graphId) {
        return String.format("%s.%s.%s", PREFIX_GAFFER_FEDERATED_STORE, graphId, "auths");
    }

    /**
     * Enum for the Graph Properties or Schema
     */
    public enum GraphConfigEnum {
        SCHEMA("schema"), PROPERTIES("properties");

        private final String value;

        GraphConfigEnum(final String value) {
            this.value = value;
        }
    }

    /**
     * Enum for the location of the {@link GraphConfigEnum}
     */
    public enum LocationEnum {
        FILE("file"), ID("id");

        private final String value;

        LocationEnum(final String value) {
            this.value = value;
        }
    }

    public String getValueOf(final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum location) {
        final String key = getGraphConfigKey(graphId, graphConfigEnum, location);
        return this.get(key);
    }

    public String getCustomPropsValue() {
        return this.get(getCustomPropsKey());
    }

    public String getGraphAuthsValue(final String graphId) {
        return this.get(getGraphAuthsKey(graphId));
    }

    public String getGraphIdsValue() {
        return this.get(KEY_GRAPH_IDS);
    }
}
