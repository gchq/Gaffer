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

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.function.Function;

/**
 * A {@code MapHandler} is a handler for the {@link Map} {@link uk.gov.gchq.gaffer.operation.Operation}
 *
 * @param <I> The object type of the input object
 * @param <O> The object type of the output object
 */
public class MapHandler<I, O> implements OutputOperationHandler<Map<I, O>, O> {

    /**
     * Handles the {@link Map} operation. Applies the function(s) contained within the Map operation
     * and returns the resulting object.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return the resulting object from the function
     * @throws OperationException if execution of the operation fails
     */
    @Override
    public O doOperation(final Map<I, O> operation, final Context context, final Store store) throws OperationException {
        if (null == operation) {
            throw new OperationException("Operation cannot be null");
        }

        Object input = operation.getInput();

        if (null == input) {
            throw new OperationException("Input cannot be null");
        }

        try {
            for (final Function function : operation.getFunctions()) {
                if (null == function) {
                    throw new OperationException("Function cannot be null");
                }

                input = function.apply(input);
            }
            return (O) input;
        } catch (final ClassCastException c) {
            throw new OperationException("The input/output types of the functions were incompatible", c);
        }
    }
}
