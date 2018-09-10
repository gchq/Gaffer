/*
 * Copyright 2018 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.ValidateOperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

/**
 * Operation Handler for ValidateOperationChain
 */
public class ValidateOperationChainHandler implements OutputOperationHandler<ValidateOperationChain, ValidationResult> {

    /**
     * Returns a {@link ValidationResult} for the supplied OperationChain.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.io.Output} operation to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return the {@link ValidationResult}
     * @throws OperationException if an error occurs handling the operation.
     */
    @Override
    public ValidationResult doOperation(final ValidateOperationChain operation, final Context context, final Store store) throws OperationException {
        return validateOperationChain(operation.getOperationChain(), context.getUser(), store);
    }

    private ValidationResult validateOperationChain(final OperationChain operationChain, final User user, final Store store) {
        return store.getOperationChainValidator().validate(operationChain, user, store);
    }
}
