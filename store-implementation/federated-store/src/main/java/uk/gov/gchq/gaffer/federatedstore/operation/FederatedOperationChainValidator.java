/*
 * Copyright 2016-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
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

    /**
     * If the given view is valid for at least 1 of the graphIds, then view is valid and exit.
     * Else none are valid, return all the errors within the validation result.
     *
     * @param op               The operation with view to test
     * @param user             The requesting user
     * @param store            The current store
     * @param validationResult The result of validation
     * @param graphIds         The graphs to test the view against
     */
    private void validateAllGraphsIdViews(final Operation op, final User user, final Store store, final ValidationResult validationResult, final Collection<String> graphIds) {
        ValidationResult savedResult = new ValidationResult();
        ValidationResult currentResult = null;

        final Operation clonedOp = shallowCloneWithDeepOptions(op);

        for (final String graphId : graphIds) {
            final boolean graphIdValid = ((FederatedStore) store).getAllGraphIds(user).contains(graphId);
            //If graphId is not valid, then there is no schema to validate a view against.
            if (graphIdValid) {
                currentResult = new ValidationResult();
                clonedOp.addOption(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, graphId);
                super.validateViews(clonedOp, user, store, currentResult);
                if (currentResult.isValid()) {
                    //If any graph has a valid View, break with valid current result
                    break;
                } else {
                    ValidationResult prependGraphId = new ValidationResult();
                    currentResult.getErrors().forEach(s -> prependGraphId.addError(String.format("(graphId: %s) %s", graphId, s)));
                    savedResult.add(prependGraphId);
                }
            }
        }

        //What state did the for loop exit with?
        if (currentResult != null && !currentResult.isValid()) {
            validationResult.addError("View is not valid for graphIds:" + graphIds.stream().collect(Collectors.joining(",", "[", "]")));
            //If invalid, no graphs views where valid, so add all saved errors.
            validationResult.add(savedResult);
        }
    }

    /**
     * Return a clone of the given operations with a deep clone of options.
     *
     * Because op.shallowClone() is used it can't be guaranteed that original options won't be modified.
     * So a deep clone of the options is made for the shallow clone of the operation.
     *
     * @param op the operation to clone
     * @return a clone of the operation with a deep clone of options.
     */
    private Operation shallowCloneWithDeepOptions(final Operation op) {
        final Operation cloneForValidation = op.shallowClone();
        final Map<String, String> options = op.getOptions();
        final Map<String, String> optionsDeepClone = isNull(options) ? null : new HashMap<>(options);
        cloneForValidation.setOptions(optionsDeepClone);
        return cloneForValidation;
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
