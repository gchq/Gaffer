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
package uk.gov.gchq.gaffer.store.operation.handler.output;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Collections;

/**
 * A {@code ToCsvHandler} handles {@link ToCsv} operations by applying the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to each item in the
 * input {@link Iterable}.
 */
public class ToCsvHandler implements OutputOperationHandler<ToCsv, Iterable<? extends String>> {
    @Override
    public Iterable<? extends String> doOperation(final ToCsv operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        if (null == operation.getElementGenerator()) {
            throw new IllegalArgumentException("ToCsv operation requires a generator");
        }

        final Iterable<? extends String> csv = operation.getElementGenerator().apply(operation.getInput());
        if (operation.isIncludeHeader()) {
            return new ChainedIterable<>(Collections.singletonList(operation.getElementGenerator().getHeader()), csv);
        }

        return csv;
    }
}
