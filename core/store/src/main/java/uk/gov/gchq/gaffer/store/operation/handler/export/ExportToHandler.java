/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.export;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.export.Exporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.Arrays;
import java.util.Collections;

/**
 * Abstract class describing how to handle {@link ExportTo} operations.
 *
 * @param <EXPORT> the {@link ExportTo} operation
 * @param <EXPORTER> the {@link Exporter} instance
 */
public abstract class ExportToHandler<EXPORT extends ExportTo, EXPORTER extends Exporter> extends ExportOperationHandler<EXPORT, EXPORTER> {
    @Override
    public Object doOperation(final EXPORT export,
                              final Context context,
                              final Store store,
                              final EXPORTER exporter)
            throws OperationException {
        final Iterable<?> inputItr = wrapInIterable(export.getInput());
        exporter.add(export.getKeyOrDefault(), inputItr);
        return export.getInput();
    }

    private Iterable<?> wrapInIterable(final Object input) {
        if (null == input) {
            return Collections.emptyList();
        }

        final Iterable inputItr;
        if (input instanceof Iterable) {
            inputItr = (Iterable) input;
        } else if (input.getClass().isArray()) {
            inputItr = Arrays.asList((Object[]) input);
        } else {
            inputItr = Collections.singleton(input);
        }
        return inputItr;
    }
}
