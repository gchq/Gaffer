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
import uk.gov.gchq.gaffer.operation.impl.ExtractItems;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A {@code ExtractItemsHandler} is a handler for the {@link ExtractItems} {@link uk.gov.gchq.gaffer.operation.Operation}.
 */
public class ExtractItemsHandler implements OutputOperationHandler<ExtractItems, Iterable<? extends Object>> {
    @Override
    public List<? extends Object> doOperation(final ExtractItems operation, final Context context, final Store store) throws OperationException {
        if (null == operation) {
            throw new OperationException("Operation cannot be null");
        }

        final Iterable<Iterable<?>> input = operation.getInput();

        if (null == input) {
            throw new OperationException("Input cannot be null");
        }

        final int selection = operation.getSelection();

        final List<Object> results = new ArrayList<>();

        for (final Iterable<? extends Object> iterable : input) {
            results.add(extract(iterable, selection));
        }

        return results;
    }

    /**
     * For a given {@link Iterable}, extracts a specific object based on the selection.
     *
     * @param input     Provided iterable from which to extract an object
     * @param selection The index of the object to extract
     * @return          The extracted object
     * @throws          OperationException if selection would be out of bounds
     */
    private Object extract(final Iterable<? extends Object> input, final int selection) throws OperationException {
        if (input instanceof List) {
            return ((List) input).get(selection);
        }

        int count = 0;
        final Iterator<?> iterator = input.iterator();
        while (count < selection) {
            if (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
                count++;
            } else {
                throw new OperationException("Selection exceeds size of iterable");
            }
        }

        return iterator.next();
    }
}
