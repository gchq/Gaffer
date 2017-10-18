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

package uk.gov.gchq.gaffer.federatedstore.operation;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;

/**
 * A FederatedOperation is an {@link FederatedOperation} that is a special
 * {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore} operation and should always be handled by the
 * {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
public interface FederatedOperation extends Operation {
    static boolean hasFederatedOperations(final OperationChain<?> operationChain) {
        for (final Operation operation : operationChain.getOperations()) {
            if (operation instanceof FederatedOperation) {
                return true;
            }
        }

        return false;
    }
}
