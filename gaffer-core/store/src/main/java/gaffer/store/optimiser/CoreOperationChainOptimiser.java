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
package gaffer.store.optimiser;

import gaffer.operation.GetOperation;
import gaffer.operation.Operation;
import gaffer.operation.Validatable;
import gaffer.operation.impl.Deduplicate;
import gaffer.operation.impl.Validate;
import gaffer.store.Store;
import java.util.Collections;
import java.util.List;

/**
 * Optimises core operations for the abstract gaffer store.
 */
public class CoreOperationChainOptimiser extends AbstractOperationChainOptimiser {
    private final Store store;

    public CoreOperationChainOptimiser(final Store store) {
        this.store = store;
    }

    /**
     * Adds validation operations for any validatable operations.
     *
     * @param previousOp the previous operation
     * @param currentOp  the current operation
     * @return the validate operation if required, otherwise an empty list.
     */
    protected List<Operation> addPreOperations(final Operation<?, ?> previousOp, final Operation<?, ?> currentOp) {
        if (doesOperationNeedValidating(previousOp, currentOp)) {
            return Collections.singletonList((Operation) createValidateOperation((Validatable<?>) currentOp));
        }
        return Collections.emptyList();
    }

    /**
     * No optimisation applied.
     *
     * @param previousOp the previous operation
     * @param currentOp  the current operation
     * @param nextOp     the next operation
     * @return singleton list containing the current operation.
     */
    protected List<Operation> optimiseCurrentOperation(final Operation<?, ?> previousOp, final Operation<?, ?> currentOp, final Operation<?, ?> nextOp) {
        return Collections.singletonList((Operation) currentOp);
    }

    /**
     * Adds deduplicate operations for any {@link GetOperation}s that have the
     * deduplicate flag set.
     *
     * @param currentOp the current operation
     * @param nextOp    the next operation
     * @return the validate operation if required, otherwise an empty list.
     */
    protected List<Operation> addPostOperations(final Operation<?, ?> currentOp, final Operation<?, ?> nextOp) {
        if (doesOperationNeedDeduplicating(currentOp, nextOp)) {
            return Collections.singletonList((Operation) createDeduplicateOperation((GetOperation<?, ?>) currentOp));
        }
        return Collections.emptyList();
    }

    /**
     * No Optimisation applied.
     *
     * @param ops operations to be optimised
     * @return the original operations.
     */
    @Override
    protected List<Operation> optimiseAll(final List<Operation> ops) {
        return ops;
    }

    private boolean doesOperationNeedValidating(final Operation<?, ?> previousOp, final Operation<?, ?> currentOp) {
        if (currentOp instanceof Validatable) {
            if (((Validatable<?>) currentOp).isValidate()) {
                return null == previousOp || !(previousOp instanceof Validate);

            }

            if (store.isValidationRequired()) {
                throw new UnsupportedOperationException("Validation is required by the store for all validatable "
                        + "operations so it cannot be disabled");
            }
        }

        return false;
    }

    private Validate createValidateOperation(final Validatable<?> currentOp) {
        final Validate validate = new Validate(currentOp.isSkipInvalidElements());
        validate.setOptions(currentOp.getOptions());

        // Move input to new validate operation
        validate.setElements(currentOp.getElements());
        currentOp.setElements(null);

        return validate;
    }

    private Deduplicate<?> createDeduplicateOperation(final GetOperation<?, ?> currentOp) {
        final Deduplicate<?> duplicate = new Deduplicate();
        duplicate.setOptions(currentOp.getOptions());
        return duplicate;
    }

    private boolean doesOperationNeedDeduplicating(final Operation<?, ?> currentOp, final Operation<?, ?> nextOp) {
        return currentOp instanceof GetOperation
                && ((GetOperation) currentOp).isDeduplicate()
                && (null == nextOp || !(nextOp instanceof Deduplicate));
    }
}
