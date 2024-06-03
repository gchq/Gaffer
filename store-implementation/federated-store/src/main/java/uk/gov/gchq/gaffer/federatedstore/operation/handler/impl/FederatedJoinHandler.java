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
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.store.operation.handler.join.JoinHandler;

public class FederatedJoinHandler<I> extends JoinHandler<I> {


    /**
     * Gets an Operation from a Join, but with a clone.
     * This avoids a false looping error being detected by the FederatedStore.
     *
     * @param join The Join to get operation from
     * @return The Operation with a clone of the options field.
     */
    @Override
    protected Operation getOperationFromJoin(final Join<I> join) {
        return super.getOperationFromJoin(join).shallowClone();
    }
}
