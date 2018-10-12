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

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.function.BinaryOperator;

/**
 * A {@code ReduceHandler} is a handler for the {@link Reduce} {@link uk.gov.gchq.gaffer.operation.Operation}
 *
 * @param <T> The object type of the input object
 */
public class ReduceHandler<T> implements OutputOperationHandler<Reduce<T>, T> {

    /**
     * Handles the {@link Reduce} operation. Applies the {@link BinaryOperator}
     * function contained within the Reduce operation and returns the resulting
     * object.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return the resulting object from the function
     * @throws OperationException if execution of the operation fails
     */
    @Override
    public T doOperation(final Reduce<T> operation, final Context context, final Store store) throws OperationException {
        if (null == operation) {
            throw new OperationException("Operation cannot be null");
        }

        Iterable<? extends T> input = operation.getInput();

        if (null == input) {
            throw new OperationException("Input cannot be null");
        }

        final T identity = operation.getIdentity();

        final BinaryOperator aggregateFunction = operation.getAggregateFunction();

        return (T) Streams.toStream(input)
                          .reduce(identity, aggregateFunction, aggregateFunction);

    }
}
