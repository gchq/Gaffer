/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.named.operation.handler;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

/**
 * Operation Handler for NamedOperation
 */
public class NamedOperationHandler implements OutputOperationHandler<NamedOperation<?, Object>, Object> {
    private NamedOperationCache cache = new NamedOperationCache();

    /**
     * Gets the requested NamedOperation, updates the input and the view, then executes the operation chain, bypassing
     * the Graph.executes graph hooks.
     *
     * @param operation the {@link Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return an object - whatever the last operation in the chain returns
     * @throws OperationException thrown when the operation fails
     */
    @Override
    public Object doOperation(final NamedOperation operation, final Context context, final Store store) throws OperationException {
        try {
            if (cache == null) {
                throw new OperationException("Cache must not be null");
            }
            NamedOperationDetail namedOperation = cache.getNamedOperation(operation.getOperationName(), context.getUser());
            OperationChain<?> operationChain = namedOperation.getOperationChain();
            operationChain = new OperationChain<>(exposeNamedOperations(operationChain, context.getUser(), cache));
            updateOperationInput(operationChain.getOperations().get(0), operation.getInput());
            operationChain = updateView(operation.getView(), operationChain);
            return store._execute(operationChain, context);
        } catch (final CacheOperationFailedException e) {
            throw new OperationException(e.getMessage(), e);
        } catch (final ClassCastException e) {
            throw new OperationException("Input type " + operation.getInput().getClass().getName() +
                    " was not valid for the operation", e);
        }
    }

    /**
     * Replaces all null views with the default view supplied to the NamedOperation. If a veiw exists in an operation
     * then the views are merged.
     *
     * @param view           the default view of the NamedOperation
     * @param operationChain the operations to later execute
     * @return the above operation chain with the views populated or merged
     */
    private OperationChain<?> updateView(final View view, final OperationChain<?> operationChain) {
        for (final Operation operation : operationChain.getOperations()) {
            if (operation instanceof OperationView) {
                final OperationView viewFilters = (OperationView) operation;
                final View opView;
                if (null == viewFilters.getView()) {
                    opView = view.clone();
                } else if (!viewFilters.getView().hasGroups()) {
                    opView = new View.Builder()
                            .merge(view)
                            .merge(viewFilters.getView())
                            .build();
                } else {
                    opView = viewFilters.getView();
                }

                opView.expandGlobalDefinitions();
                viewFilters.setView(opView);
            }
        }
        return operationChain;
    }

    /**
     * Injects the input of the NamedOperation into the first operation in the OperationChain. This is used when
     * chaining NamedOperations together.
     *
     * @param op    the first operation in a chain
     * @param input the input of the NamedOperation
     */
    private void updateOperationInput(final Operation op, final Object input) {
        if (null != input && (op instanceof Input) && null == ((Input) op).getInput()) {
            ((Input) op).setInput(input);
        }
    }

    private List<Operation> exposeNamedOperations(final OperationChain<?> opChain, final User user, final NamedOperationCache cache) throws CacheOperationFailedException {
        ArrayList<Operation> operations = new ArrayList<>();
        for (final Operation operation : opChain.getOperations()) {
            if (operation instanceof NamedOperation) {
                final NamedOperation namedOp = (NamedOperation) operation;
                OperationChain<?> innerChain = cache.getNamedOperation(namedOp.getOperationName(), user).getOperationChain();
                updateOperationInput(innerChain.getOperations().get(0), namedOp.getInput());
                operations.addAll(exposeNamedOperations(innerChain, user, cache));
            } else {
                operations.add(operation);
            }
        }
        return operations;

    }

    public NamedOperationCache getCache() {
        return cache;
    }

    public void setCache(final NamedOperationCache cache) {
        this.cache = cache;
    }
}
