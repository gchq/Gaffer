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

import java.util.Arrays;
import java.util.Iterator;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.PREFIX_GAFFER_FEDERATED_STORE;


/**
 * Additional {@link StoreProperties} for the {@link FederatedStore}.
 */
public class FederatedStoreProperties extends StoreProperties {
    private static final String CUSTOM_PROPERTIES_AUTHS = "customPropertiesAuths";
    private static final char DELIMITER = '.';
    private static final String GRAPH_IDS = "graphIds";
    private static final String AUTHS = "auths";

    public FederatedStoreProperties() {
        setStoreClass(FederatedStore.class);
    }

    public void setGraphIds(final String graphIdsCSV) {
        set(getKeyGraphIds(), graphIdsCSV);
    }

    private String getKeyGraphIds() {
        return getPrefixAnd(GRAPH_IDS);
    }

    public void setTrueSkipFailedExecution() {
        setSkipFailedExecution(true);
    }

    public void setFalseSkipFailedExecution() {
        setSkipFailedExecution(false);
    }

    public void setCustomPropertyAuths(final String auths) {
        set(getKeyCustomProps(), auths);
    }

    private static String getKeyCustomProps() {
        return getPrefixAnd(CUSTOM_PROPERTIES_AUTHS);
    }

    private static String getPrefixAnd(final String... affix) {
        final StringBuilder sb = new StringBuilder()
                .append(PREFIX_GAFFER_FEDERATED_STORE)
                .append(DELIMITER);
        final Iterator<String> iterator = Arrays.asList(affix).iterator();
        while (iterator.hasNext()) {
            sb.append(iterator.next());
            if (iterator.hasNext()) {
                sb.append(DELIMITER);
            }
        }
        return sb.toString();
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
        return getPrefixAnd(graphId, graphConfigEnum.value, locationEnum.value);
    }


    private static String getKeyGraphAuths(final String graphId) {
        return getPrefixAnd(graphId, AUTHS);
    }

    public String getGraphIsPublicValue(final String graphId) {
        return get(getKeyGraphIsPublic(graphId));
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
        return getPrefixAnd(graphId, "isPublic");
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
        return this.get(getKeyCustomProps());
    }

    public String getGraphAuthsValue(final String graphId) {
        return this.get(getKeyGraphAuths(graphId));
    }

    public String getGraphIdsValue() {
        return this.get(getKeyGraphIds());
    }

    public String getKeyGraphsCanHavePublicAccess() {
        return this.getPrefixAnd("graphsCanHavePublicAccess");
    }

    public String getGraphsCanHavePublicAccessValue() {
        return get(getKeyGraphsCanHavePublicAccess());
    }

    public void setFalseGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(false);
    }

    public void setTrueGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(true);
    }

    public void setGraphsCanHavePublicAccess(final boolean b) {
        set(getKeyGraphsCanHavePublicAccess(), Boolean.toString(b));
    }
}
