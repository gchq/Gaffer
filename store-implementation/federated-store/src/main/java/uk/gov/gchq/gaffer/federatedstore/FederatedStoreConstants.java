/*
 * Copyright 2016 Crown Copyright
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

public final class FederatedStoreConstants {
    // Operation options
    public static final String GAFFER_FEDERATED_STORE = "gaffer.federatedstore";
    public static final String GRAPH_IDS = "graphIds";
    public static final String KEY_OPERATION_OPTIONS_GRAPH_IDS = GAFFER_FEDERATED_STORE + "operation.graphIds";
    public static final String KEY_GRAPH_IDS = GAFFER_FEDERATED_STORE + "." + GRAPH_IDS;
    public static final String KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE = "gaffer.federatedstore.operation.skipFailedFederatedStoreExecute";
    public static final String FILE = "file";
    public static final String SCHEMA = "schema";
    public static final String PROPERTIES = "properties";
    public static final String ID = "id";
    public static final String CUSTOM_PROPERTIES_AUTHS = "customPropertiesAuths";
    public static final String AUTHS = "auths";

    private FederatedStoreConstants() {
        // private constructor to prevent users instantiating this class as it
        // only contains constants.
    }
}
