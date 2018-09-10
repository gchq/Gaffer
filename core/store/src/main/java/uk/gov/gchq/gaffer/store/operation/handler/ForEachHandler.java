/*
 * Copyright 2018 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.List;

public class ForEachHandler<T, U> implements OutputOperationHandler<ForEach<T, U>, Iterable<? extends U>> {

    @Override
    public Iterable<? extends U> doOperation(final ForEach operation, final Context context, final Store store) throws OperationException {
        return runForEach(operation.getInput(), operation.getOperation(), store, context);
    }

    private Iterable<U> runForEach(final Iterable<Object> input, Operation operation, final Store store, final Context context) throws OperationException {
        List output = new ArrayList<>();

        if (null == operation) {
            throw new OperationException("Operation cannot be null");
        }
        if (null == input) {
            throw new OperationException("Input cannot be null");
        }

        if (operation instanceof Input) {
            ((Input) operation).setInput(input);
        }

        if (operation instanceof Output) {
            output.add(store.execute((Output) operation, context));
        }
        return output;
    }
}
