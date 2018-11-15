/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.getResultsOrNull;
import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

/**
 * An operation handler for {@link If} operations.
 * If {@link If#getThen()} or {@link If#getOtherwise()} is called but returns a null operation,
 * then the input object will simply be returned.
 */
public class IfHandler implements OutputOperationHandler<If<Object, Object>, Object> {
    @Override
    public Object doOperation(final If operation, final Context context, final Store store) throws OperationException {
        final Object input = operation.getInput();

        boolean computedCondition;

        if (null == operation.getCondition()) {
            if (null == operation.getConditional() || null == operation.getConditional().getPredicate()) {
                computedCondition = false;
            } else {
                final Object intermediate;
                if (null == operation.getConditional().getTransform()) {
                    intermediate = input;
                } else {
                    final Operation transform = operation.getConditional().getTransform();
                    updateOperationInput(transform, input);
                    intermediate = getResultsOrNull(transform, context, store);
                }
                try {
                    computedCondition = operation.getConditional().getPredicate().test(intermediate);
                } catch (final ClassCastException e) {
                    final String inputType = null != intermediate ? intermediate.getClass().getSimpleName() : "null";
                    throw new OperationException("The predicate '" + operation.getConditional().getPredicate().getClass().getSimpleName()
                            + "' cannot accept an input of type '" + inputType + "'");
                }
            }
        } else {
            computedCondition = operation.getCondition();
        }

        final Operation nextOp = computedCondition ? operation.getThen() : operation.getOtherwise();
        if (null == nextOp) {
            return input;
        } else {
            updateOperationInput(nextOp, input);
            return getResultsOrNull(nextOp, context, store);
        }
    }
}
