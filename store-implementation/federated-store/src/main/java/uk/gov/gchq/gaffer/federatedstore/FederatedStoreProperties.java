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

import java.io.InputStream;
import java.nio.file.Path;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.PREFIX_GAFFER_FEDERATED_STORE;


/**
 * Additional {@link StoreProperties} for the {@link FederatedStore}.
 */
public class FederatedStoreProperties extends StoreProperties {
    /**
     * This is used....
     * e.g gaffer.federatedstore.isPublicAllowed=true
     */
    public static final String IS_PUBLIC_ACCESS_ALLOWED = "gaffer.federatedstore.isPublicAllowed";
    public static final String IS_PUBLIC_ACCESS_ALLOWED_DEFAULT = String.valueOf(true);
    /**
     * This is used....
     * e.g gaffer.federatedstore.customPropertiesAuths="auth1"
     */
    public static final String CUSTOM_PROPERTIES_AUTHS = "gaffer.federatedstore.customPropertiesAuths";
    public static final String CUSTOM_PROPERTIES_AUTHS_DEFAULT = null;
    /**
     * This is used....
     * e.g gaffer.federatedstore.graphIds=graph1,graph2
     */
    public static final String GRAPH_IDS = "gaffer.federatedstore.graphIds";
    public static final String GRAPH_IDS_DEFAULT = null;

    /**
     * This is used....
     * e.g gaffer.federatedstore.graph1.auths=auth1,auth2
     */
    private static final String AUTHS = "auths";

    /**
     * This is used....
     * e.g gaffer.federatedstore.graph1.isPublic=false
     */
    private static final String IS_PUBLIC = "isPublic";
    public static final String IS_PUBLIC_DEFAULT = String.valueOf(false);

    public FederatedStoreProperties() {
        super(FederatedStore.class);
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

    public void setGraphIds(final String graphIdsCSV) {
        set(GRAPH_IDS, graphIdsCSV);
    }

    public void setTrueSkipFailedExecution() {
        setSkipFailedExecution(true);
    }

    public void setFalseSkipFailedExecution() {
        setSkipFailedExecution(false);
    }

    public void setCustomPropertyAuths(final String auths) {
        set(CUSTOM_PROPERTIES_AUTHS, auths);
    }

    public void setGraphAuth(final String graphId, final String authCSV) {
        set(getKeyGraphAuths(graphId), authCSV);
    }

    public void setSkipFailedExecution(final boolean b) {
        set(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE, Boolean.toString(b));
    }

    public void setGraphPropFile(final String graphId, final String file) {
        final String key = getKeyGraphConfig(graphId, GraphConfigEnum.PROPERTIES, LocationEnum.FILE);
        set(key, file);
    }

    public void setGraphSchemaFile(final String graphId, final String file) {
        final String key = getKeyGraphConfig(graphId, GraphConfigEnum.SCHEMA, LocationEnum.FILE);
        set(key, file);
    }

    public void setGraphPropId(final String graphId, final String id) {
        final String key = getKeyGraphConfig(graphId, GraphConfigEnum.PROPERTIES, LocationEnum.ID);
        set(key, id);
    }

    public void setGraphSchemaId(final String graphId, final String id) {
        final String key = getKeyGraphConfig(graphId, GraphConfigEnum.SCHEMA, LocationEnum.ID);
        set(key, id);
    }

    private static String getKeyGraphConfig(final String graphId, final GraphConfigEnum graphConfigEnum, final LocationEnum locationEnum) {
        return String.format("%s.%s.%s.%s", PREFIX_GAFFER_FEDERATED_STORE, graphId, graphConfigEnum.value, locationEnum.value);
    }

    private static String getKeyGraphAuths(final String graphId) {
        return String.format("%s.%s.%s", PREFIX_GAFFER_FEDERATED_STORE, graphId, AUTHS);
    }

    public String getGraphIsPublicValue(final String graphId) {
        return get(getKeyGraphIsPublic(graphId), IS_PUBLIC_DEFAULT);
    }

    public void setGraphIsPublicValue(final String graphId, final boolean b) {
        set(getKeyGraphIsPublic(graphId), String.valueOf(b));
    }

    public void setTrueGraphIsPublicValue(final String graphId) {
        setGraphIsPublicValue(graphId, true);
    }

    public void setFalseGraphIsPublicValue(final String graphId) {
        setGraphIsPublicValue(graphId, false);
    }

    private String getKeyGraphIsPublic(final String graphId) {
        return String.format("%s.%s.%s", PREFIX_GAFFER_FEDERATED_STORE, graphId, IS_PUBLIC);
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
        final String key = getKeyGraphConfig(graphId, graphConfigEnum, location);
        return this.get(key);
    }

    public String getCustomPropsValue() {
        return this.get(CUSTOM_PROPERTIES_AUTHS, CUSTOM_PROPERTIES_AUTHS_DEFAULT);
    }

    public String getGraphAuthsValue(final String graphId) {
        return this.get(getKeyGraphAuths(graphId));
    }

    public String getGraphIdsValue() {
        return this.get(GRAPH_IDS, GRAPH_IDS_DEFAULT);
    }

    public String getIsPublicAccessAllowed() {
        return get(IS_PUBLIC_ACCESS_ALLOWED, IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);
    }

    public void setFalseGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(false);
    }

    public void setTrueGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(true);
    }

    public void setGraphsCanHavePublicAccess(final boolean b) {
        set(IS_PUBLIC_ACCESS_ALLOWED, Boolean.toString(b));
    }
}
