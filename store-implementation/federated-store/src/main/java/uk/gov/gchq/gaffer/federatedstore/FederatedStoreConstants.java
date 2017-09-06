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
    public static final String GRAPH_IDS = "gaffer.federatedstore.operation.graphIds";
    public static final String SKIP_FAILED_FEDERATED_STORE_EXECUTE = "gaffer.federatedstore.operation.skipFailedFederatedStoreExecute";

    private FederatedStoreConstants() {
        // private constructor to prevent users instantiating this class as it
        // only contains constants.
    }
}
