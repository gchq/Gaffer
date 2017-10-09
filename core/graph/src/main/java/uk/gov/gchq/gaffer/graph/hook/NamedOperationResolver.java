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

package uk.gov.gchq.gaffer.graph.hook;

import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link GraphHook} to resolve named operations.
 */
public class NamedOperationResolver implements GraphHook {
    private final NamedOperationCache cache;

    public NamedOperationResolver() {
        this(new NamedOperationCache());
    }

    public NamedOperationResolver(final NamedOperationCache cache) {
        this.cache = cache;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        final List<Operation> updatedOperations = resolveNamedOperations(opChain.getOperations(), context.getUser());
        opChain.getOperations().clear();
        opChain.getOperations().addAll(updatedOperations);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    private List<Operation> resolveNamedOperations(final List<Operation> operations, final User user) {
        List<Operation> updatedOperations = new ArrayList<>(operations.size());
        for (final Operation operation : operations) {
            if (NamedOperation.class.equals(operation.getClass())) {
                updatedOperations.addAll(resolveNamedOperation((NamedOperation) operation, user));
            } else {
                updatedOperations.add(operation);
            }
        }
        return updatedOperations;
    }

    private List<Operation> resolveNamedOperation(final NamedOperation namedOp, final User user) {
        final NamedOperationDetail namedOpDetail;
        try {
            namedOpDetail = cache.getNamedOperation(namedOp.getOperationName(), user);
        } catch (final CacheOperationFailedException e) {
            // Unable to find named operation - just return the original named operation
            return Collections.singletonList(namedOp);
        }

        final OperationChain<?> namedOperationChain = namedOpDetail.getOperationChain(namedOp.getParameters());
        updateOperationInput(namedOperationChain, namedOp.getInput());

        // Call resolveNamedOperations again to check there are no nested named operations
        return resolveNamedOperations(namedOperationChain.getOperations(), user);
    }

    /**
     * Injects the input of the NamedOperation into the first operation in the OperationChain. This is used when
     * chaining NamedOperations together.
     *
     * @param opChain the resolved operation chain
     * @param input   the input of the NamedOperation
     */
    private void updateOperationInput(final OperationChain<?> opChain, final Object input) {
        final Operation firstOp = opChain.getOperations().get(0);
        if (null != input && (firstOp instanceof Input) && null == ((Input) firstOp).getInput()) {
            ((Input) firstOp).setInput(input);
        }
    }
}
