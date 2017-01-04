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
package uk.gov.gchq.gaffer.store.optimiser;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractOperationChainOptimiser implements OperationChainOptimiser {
    @Override
    public final <OUTPUT> OperationChain<OUTPUT> optimise(final OperationChain<OUTPUT> operationChain) {
        final List<Operation> ops = operationChain.getOperations();
        final int numOps = ops.size();

        if (numOps == 0) {
            return operationChain;
        }

        final List<Operation> optimisedOps = new ArrayList<>();
        Operation<?, ?> previousOp;
        Operation<?, ?> currentOp = null;
        Operation<?, ?> nextOp = ops.get(0);
        for (int index = 0; index < numOps; index++) {
            previousOp = currentOp;
            currentOp = nextOp;
            nextOp = ((index + 1) < numOps) ? ops.get(index + 1) : null;

            optimisedOps.addAll(addPreOperations(previousOp, currentOp));
            optimisedOps.addAll(optimiseCurrentOperation(previousOp, currentOp, nextOp));
            optimisedOps.addAll(addPostOperations(currentOp, nextOp));
        }

        return new OperationChain<>(optimiseAll(optimisedOps));
    }

    /**
     * Add pre operations. By default this should just return an empty list.
     *
     * @param previousOp the previous operation
     * @param currentOp  the current operation
     * @return list of pre operations
     */
    protected abstract List<Operation> addPreOperations(final Operation<?, ?> previousOp, final Operation<?, ?> currentOp);

    /**
     * Optimises the current operation. By default this should just return the current operation.
     *
     * @param previousOp the previous operation
     * @param currentOp  the current operation
     * @param nextOp     the next operation
     * @return list of optimised operations
     */
    protected abstract List<Operation> optimiseCurrentOperation(final Operation<?, ?> previousOp, final Operation<?, ?> currentOp, final Operation<?, ?> nextOp);

    /**
     * Add pre operations. By default this should just return an empty list.
     *
     * @param currentOp the current operation
     * @param nextOp    the next operation
     * @return list of post operations
     */
    protected abstract List<Operation> addPostOperations(final Operation<?, ?> currentOp, final Operation<?, ?> nextOp);

    /**
     * Optimise all operations together.
     *
     * @param ops operations to be optimised
     * @return the optimised operations
     */
    protected abstract List<Operation> optimiseAll(final List<Operation> ops);
}
