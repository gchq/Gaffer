/*
 * Copyright 2016-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.shallowCloneWithDeepOptions;

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
    protected Schema getSchema(final Operation op, final User user, final Store store) {
        try {
            return (op instanceof FederatedOperation)
                    ? store.execute(new FederatedOperation.Builder().<Void, Schema>op(new GetSchema()).graphIds(((FederatedOperation<?, ?>) op).getGraphIds()).build(), new Context(user))
                    : store.execute(new GetSchema(), new Context(user));
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Unable to execute GetSchema Operation", e);
        }
    }

    @Override
    protected boolean shouldValidate(final Operation op) {
        return super.shouldValidate(op) || (op instanceof FederatedOperation && super.shouldValidate(((FederatedOperation) op).getPayloadOperation()));
    }

    @Override
    protected View getView(final Operation op) {
        return op instanceof FederatedOperation
                ? super.getView(((FederatedOperation) op).getPayloadOperation())
                : super.getView(op);
    }


    /**
     * If the given view is valid for at least 1 of the graphIds, then view is valid and exit.
     * Else none are valid, return all the errors within the validation result.
     *
     * @param op               The operation with view to test
     * @param user             The requesting user
     * @param store            The current store
     * @param validationResult The result of validation
     */
    @Override
    protected void validateViews(final Operation op, final User user, final Store store, final ValidationResult validationResult) {
        ValidationResult savedResult = new ValidationResult();
        ValidationResult currentResult = null;

        if (op instanceof FederatedOperation || !(op instanceof IFederationOperation)) {
            final List<String> graphIds = getGraphIds(op, user, (FederatedStore) store);
            FederatedOperation clonedOp = op instanceof FederatedOperation
                    ? ((FederatedOperation) op).deepClone()
                    : new FederatedOperation
                    .Builder()
                    .op(shallowCloneWithDeepOptions(op))
                    .graphIds(graphIds)
                    .setUserRequestingAdminUsage(op instanceof IFederationOperation && ((IFederationOperation) op).isUserRequestingAdminUsage())
                    .build();
            Collection<GraphSerialisable> graphSerialisables = ((FederatedStore) store).getGraphs(user, graphIds, clonedOp);
            for (final GraphSerialisable graphSerialisable : graphSerialisables) {
                String graphId = graphSerialisable.getGraphId();
                final boolean graphIdValid = ((FederatedStore) store).getAllGraphIds(user).contains(graphId);
                // If graphId is not valid, then there is no schema to validate a view against.
                if (graphIdValid) {
                    currentResult = new ValidationResult();
                    clonedOp.graphIdsCSV(graphId);
                    super.validateViews(clonedOp, user, store, currentResult);
                    if (currentResult.isValid()) {
                        // If any graph has a valid View, break with valid current result
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

                validationResult.addError("View is not valid for graphIds:" + (graphIds == null ? new ArrayList<String>() : graphIds).stream().collect(Collectors.joining(",", "[", "]")));
                //If invalid, no graphs views where valid, so add all saved errors.
                validationResult.add(savedResult);
            }
        }
    }

    private List<String> getGraphIds(final Operation op, final User user, final FederatedStore store) {

        final List<String> allGraphIds = store.getAllGraphIds(user, op instanceof IFederationOperation && ((IFederationOperation) op).isUserRequestingAdminUsage());

        final List<String> graphIdsFromOperation = (op instanceof IFederatedOperation) ? ((IFederatedOperation) op).getGraphIds() : new ArrayList<String>();

        return (nonNull(graphIdsFromOperation) && !graphIdsFromOperation.isEmpty())
                ? allGraphIds.stream().filter(graphIdsFromOperation::contains).collect(Collectors.toList())
                : allGraphIds;

    }
}
