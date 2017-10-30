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
package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.FlatMap;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A {@code FlatMap} is a handler for the {@link FlatMap} {@link uk.gov.gchq.gaffer.operation.Operation}
 *
 * @param <I_ITEM> The object type of the nested iterables
 * @param <O_ITEM> The object type of the output iterable
 */
public class FlatMapHandler<I_ITEM, O_ITEM> implements OutputOperationHandler<FlatMap<I_ITEM, O_ITEM>, Iterable<O_ITEM>> {

    /**
     * Handles the {@link FlatMap} operation. For each iterable in the input iterable, the function
     * contained within the FlatMap will be applied, and the results returned as a List.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return an iterable of objects
     * @throws OperationException if execution of the operation fails
     */
    @Override
    public Iterable<O_ITEM> doOperation(final FlatMap<I_ITEM, O_ITEM> operation, final Context context, final Store store) throws OperationException {
        if (null == operation) {
            throw new OperationException("Operation cannot be null");
        }

        final Iterable<Iterable<I_ITEM>> input = operation.getInput();

        if (null == input) {
            throw new OperationException("Input cannot be null");
        }

        final Function<Iterable<I_ITEM>, O_ITEM> function = operation.getFunction();

        final List<O_ITEM> results = new ArrayList<>();

        for (Iterable<I_ITEM> iterable : input) {
            results.add(function.apply(iterable));
        }

        return results;
    }
}
