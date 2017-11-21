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

import uk.gov.gchq.gaffer.operation.Operation;

public final class FederatedStoreConstants {
    // Operation options
    public static final String KEY_OPERATION_OPTIONS_GRAPH_IDS = "gaffer.federatedstore.operation.graphIds";
    public static final String KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE = "gaffer.federatedstore.operation.skipFailedFederatedStoreExecute";
    public static final String DEFAULT_VALUE_KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE = String.valueOf(false);
    public static final String DEFAULT_VALUE_IS_PUBLIC = String.valueOf(false);

    private FederatedStoreConstants() {
        // private constructor to prevent users instantiating this class as it
        // only contains constants.
    }

    public static String getSkipFailedFederatedStoreExecute(final Operation op) {
        return op.getOption(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE, DEFAULT_VALUE_KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE);
    }
}
