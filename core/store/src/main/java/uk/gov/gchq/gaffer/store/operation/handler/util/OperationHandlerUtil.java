/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.util;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * Utilities for Operation Handlers.
 */
public final class OperationHandlerUtil {
    private OperationHandlerUtil() {
    }

    /**
     * <p>
     * Updates the input of an operation if the operation is an OperationChain
     * or an Input.
     * </p>
     * <p>
     * If the operation is an operation chain then the input will be set on
     * the first operation if it is an Input operation with a null input.
     * </p>
     * <p>
     * If the operation is an Input operation then the input will be set
     * if the current input is null.
     * </p>
     *
     * @param operation the operation to update
     * @param input     the new input to set on the operation.
     */
    public static void updateOperationInput(final Operation operation, final Object input) {
        if (operation instanceof OperationChain) {
            if (!((OperationChain) operation).getOperations().isEmpty()) {
                final Operation firstOp = (Operation) ((OperationChain) operation).getOperations().get(0);
                if (firstOp instanceof Input) {
                    setOperationInput(firstOp, input);
                }
            }
        } else if (operation instanceof Input) {
            setOperationInput(operation, input);
        }
    }

    private static void setOperationInput(final Operation operation, final Object input) {
        if (null == ((Input) operation).getInput()) {
            ((Input) operation).setInput(input);
        }
    }

    /**
     * Executes and operation on the store and returns the results or null.
     *
     * @param op      the operation to execute
     * @param context the user context
     * @param store   the store to execute the operation on
     * @return the results or null
     * @throws OperationException if the store fails to execute the operation.
     */
    public static Object getResultsOrNull(final Operation op, final Context context, final Store store) throws OperationException {
        if (op instanceof Output) {
            return store.execute((Output) op, context);
        } else {
            store.execute(op, context);
            return null;
        }
    }
}
