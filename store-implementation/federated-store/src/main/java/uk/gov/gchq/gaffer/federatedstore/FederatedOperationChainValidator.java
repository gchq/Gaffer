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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Objects.nonNull;

/**
 * Validation class for validating {@link uk.gov.gchq.gaffer.operation.OperationChain}s against {@link ViewValidator}s using the Federated Store schemas.
 * Extends {@link OperationChainValidator} and uses the {@link FederatedStore} to get
 * the merged schema based on the user context and operation options.
 */
public class FederatedOperationChainValidator extends OperationChainValidator {
    public FederatedOperationChainValidator(final ViewValidator viewValidator) {
        super(viewValidator);
    }

    @Override
    protected Schema getSchema(final Operation operation, final User user, final Store store) {
        return ((FederatedStore) store).getSchema(operation, user);
    }

    @Override
    protected void validateViews(final Operation op, final User user, final Store store, final ValidationResult validationResult) {
        validateAllGraphsIdViews(op, user, store, validationResult, getGraphIds(op, user, (FederatedStore) store));
    }

    private void validateAllGraphsIdViews(final Operation op, final User user, final Store store, final ValidationResult validationResult, final Collection<String> graphIds) {
        ValidationResult errors = new ValidationResult();
        ValidationResult current = errors;

        for (final String graphId : graphIds) {
            current = new ValidationResult();
            final Operation clone = op.shallowClone();
            clone.addOption(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, graphId);
            super.validateViews(clone, user, store, current);
            if (current.isValid()) {
                break;
            } else {
                errors.add(current);
            }
        }

        if (!current.isValid()) {
            validationResult.add(errors);
        }
    }

    private Collection<String> getGraphIds(final Operation op, final User user, final FederatedStore store) {
        return nonNull(op) && nonNull(getGraphIds(op)) && !getGraphIds(op).isEmpty()
                ? Arrays.asList(getGraphIds(op).split(","))
                : store.getAllGraphIds(user);
    }

    private String getGraphIds(final Operation op) {
        return op.getOption(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS);
    }
}
