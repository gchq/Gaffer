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
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link GraphHook} to resolve named operations.
 */
public class NamedOperationGraphHook implements GraphHook {
    private final NamedOperationCache cache;

    public NamedOperationGraphHook() {
        this(new NamedOperationCache());
    }

    public NamedOperationGraphHook(final NamedOperationCache cache) {
        this.cache = cache;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        final List<Operation> updatedOperations = resolveNamedOperations(opChain.getOperations(), user);
        opChain.getOperations().clear();
        opChain.getOperations().addAll(updatedOperations);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final User user) {
        return result;
    }

    private List<Operation> resolveNamedOperations(final List<Operation> operations, final User user) {
        List<Operation> updatedOperations = new ArrayList<>(operations.size());
        for (final Operation operation : operations) {
            if (operation instanceof NamedOperation) {
                updatedOperations.addAll(resolveNamedOperation((NamedOperation) operation, user));
            } else {
                updatedOperations.add(operation);
            }
        }
        return updatedOperations;
    }

    private List<Operation> resolveNamedOperation(final NamedOperation operation, final User user) {
        try {
            final NamedOperationDetail namedOperation = cache.getNamedOperation(operation.getOperationName(), user);
            final OperationChain<?> namedOperationChain = namedOperation.getOperationChain(operation.getParameters());
            // Call resolveNamedOperations again to check there are no nested named operations
            return resolveNamedOperations(namedOperationChain.getOperations(), user);
        } catch (final CacheOperationFailedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
