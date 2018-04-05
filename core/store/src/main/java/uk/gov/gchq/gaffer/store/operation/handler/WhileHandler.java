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
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.getResultsOrNull;
import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

/**
 * An operation handler for {@link While} operations.
 * <p>
 * The default handler has a maxRepeats field that can be overridden by system
 * administrators. The default value is set to 1000. To update this value,
 * create an operation declarations JSON file containing the While operation
 * and your configured WhileHandler. E.g:
 * <pre>
 * {
 *     "operations": [
 *         {
 *             "operation": "uk.gov.gchq.gaffer.operation.impl.While",
 *             "handler": {
 *                 "class": "uk.gov.gchq.gaffer.store.operation.handler.WhileHandler",
 *                 "maxRepeats": 10
 *             }
 *         }
 *     ]
 * }
 * </pre>
 * and then register a path to the json file in your store properties
 * using the key gaffer.store.operation.declarations.
 * <p>
 */
public class WhileHandler implements OutputOperationHandler<While<Object, Object>, Object> {
    private int maxRepeats = While.MAX_REPEATS;

    @Override
    public Object doOperation(final While operation,
                              final Context context,
                              final Store store) throws OperationException {
        validateMaxRepeats(operation);

        Object input = operation.getInput();
        for (int repeatCount = 0; repeatCount < operation.getMaxRepeats(); repeatCount++) {
            final While operationClone = operation.shallowClone();
            if (!isSatisfied(input, operationClone, context, store)) {
                break;
            }
            input = doDelegateOperation(input, operationClone.getOperation(), context, store);
        }

        return input;
    }

    public void validateMaxRepeats(final While operation) throws OperationException {
        if (operation.getMaxRepeats() > maxRepeats) {
            throw new OperationException("Max repeats of the While operation is too large: "
                    + operation.getMaxRepeats() + " > " + maxRepeats);
        }
    }

    public Object doDelegateOperation(final Object input, final Operation delegate, final Context context, final Store store) throws OperationException {
        updateOperationInput(delegate, input);
        return getResultsOrNull(delegate, context, store);
    }

    public boolean isSatisfied(final Object input,
                               final While operation,
                               final Context context,
                               final Store store) throws OperationException {
        final boolean satisfied;
        if (null == operation.getConditional()) {
            satisfied = null == operation.isCondition() || operation.isCondition();
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
                satisfied = operation.getConditional().getPredicate().test(intermediate);
            } catch (final ClassCastException e) {
                final String inputType = null != input ? input.getClass().getSimpleName() : "null";
                throw new OperationException("The predicate '" + operation.getConditional().getPredicate().getClass().getSimpleName()
                        + "' cannot accept an input of type '" + inputType + "'");
            }
        }
        return satisfied;
    }

    public int getMaxRepeats() {
        return maxRepeats;
    }

    public void setMaxRepeats(final int maxRepeats) {
        this.maxRepeats = maxRepeats;
    }
}
