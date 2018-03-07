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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Repeat;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.OperationConstants;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

/**
 * A {@code RepeatHandler} is a handler for {@link Repeat} operations.
 *
 * This handler can be configured with a maximum number of repeats, with the default being 20.
 *
 * If the delegate operation is an implementation of {@link Output}, the output value from each
 * operation is passed as the input to the next. Otherwise, the delegate operation is just
 * executed on the store for the configured number of repeats.
 */
public class RepeatHandler implements OutputOperationHandler<Repeat, Object> {

    @Override
    public Object doOperation(final Repeat operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Input cannot be null");
        }

        final int maxTimes = OperationConstants.MAX_REPEATS_DEFAULT;
        if (maxTimes < operation.getTimes()) {
            throw new OperationException("Maximum number of allowed repeats: " + maxTimes + " exceeded: " + operation.getTimes());
        }

        Object input = operation.getInput();
        final int times = operation.getTimes();
        final Operation delegate = operation.getOperation();
        for (int i = 0; i < times; i++) {
            if (delegate instanceof Output) {
                input = store.execute((Output) delegate, context);
            } else {
                store.execute(delegate, context);
            }
        }
        return input;
    }
}
