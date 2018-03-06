/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * An {@code OperationHandler} defines how to handle a specific {@link uk.gov.gchq.gaffer.operation.Operation}.
 * All instances of an OperationHandler must be stateless as they are reused for all concurrent operations.
 */
public interface OperationHandler<OP extends Operation> {
    /**
     * Execute the given {@link uk.gov.gchq.gaffer.operation.Operation}.
     *
     * @param operation the {@link Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return the output for the operation or null.
     * @throws OperationException thrown if the operation fails
     */
    Object doOperation(final OP operation, final Context context, final Store store) throws OperationException;
}
