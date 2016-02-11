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

package gaffer.store.operation.handler;

import gaffer.operation.Operation;
import gaffer.operation.OperationException;
import gaffer.store.Store;

/**
 * An <code>OperationHandler</code> defines how to handle a specific {@link gaffer.operation.Operation}.
 */
public interface OperationHandler<OPERATION extends Operation<?, ?>, OUTPUT> {
    /**
     * Execute the given {@link gaffer.operation.Operation}.
     *
     * @param operation the {@link gaffer.operation.Operation} to be executed
     * @param store     the {@link gaffer.store.Store} the operation should be run on
     * @return the OUTPUT for the operation.
     * @throws OperationException thrown if the operation fails
     */
    OUTPUT doOperation(final OPERATION operation, final Store store) throws OperationException;
}
