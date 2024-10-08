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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.store.operation.handler.WhileHandler;

/**
 * <p>
 * An operation handler for {@link While} operations.
 *
 * @see WhileHandler
 */
public class FederatedWhileHandler extends WhileHandler {

    /**
     * Gets an Operation from a While, but with a clone.
     * This avoids a false looping error being detected by the FederatedStore.
     *
     * @param aWhile The while to get operation from
     * @return The Operation with a clone of the options field.
     */
    @Override
    protected Operation getOperationFromWhile(final While aWhile) {
        return super.getOperationFromWhile(aWhile).shallowClone();
    }

}
