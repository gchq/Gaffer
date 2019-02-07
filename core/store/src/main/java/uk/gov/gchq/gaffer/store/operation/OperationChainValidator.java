/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

/**
 * Validation class for validating {@link OperationChain}s against {@link ViewValidator}s.
 */
public class OperationChainValidator extends OperationValidator {

    public OperationChainValidator(final ViewValidator viewValidator) {
        super(viewValidator);
    }

    /**
     * Validate the provided {@link OperationChain} against the {@link ViewValidator}.
     *
     * @param operationChain the operation chain to validate
     * @param user           the user making the request
     * @param store          the target store
     * @return the {@link ValidationResult}
     */
    public ValidationResult validate(final OperationChain<?> operationChain, final User user, final Store store) {
        final ValidationResult validationResult = new ValidationResult();
        if (operationChain.getOperations().isEmpty()) {
            validationResult.addError("Operation chain contains no operations");
        } else {
            Class<? extends Output> output = null;
            for (final Operation op : operationChain.getOperations()) {
                output = validate(op, user, store, validationResult, output);
            }
        }
        return validationResult;
    }
}
