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

import uk.gov.gchq.gaffer.commonutil.iterable.LazyFunctionIterator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.IterableMap;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.function.Function;

public class IterableMapHandler<I_ITEM, O_ITEM> implements
        OutputOperationHandler<IterableMap<I_ITEM, O_ITEM>, Iterable<? extends O_ITEM>> {
    @Override
    public Iterable<? extends O_ITEM> doOperation(final IterableMap<I_ITEM, O_ITEM> operation, final Context context, final Store store) throws OperationException {
        if (null == operation) {
            throw new OperationException("Operation cannot be null");
        }

        final Iterable<? extends I_ITEM> input = operation.getInput();

        if (null == input) {
            throw new OperationException("Input cannot be null");
        }

        final Function<I_ITEM, O_ITEM> function = operation.getFunction();

        if (null == function) {
            throw new OperationException("Operation cannot be null");
        }

        final LazyFunctionIterator<I_ITEM, O_ITEM> iterator = new LazyFunctionIterator<>();

        return iterator.applyFunction(input, function);
    }
}
