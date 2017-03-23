/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.impl.export.Export;
import uk.gov.gchq.gaffer.operation.impl.export.Exporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import java.util.Arrays;
import java.util.Collections;

public abstract class ExportHandler<EXPORT extends Export, EXPORTER extends Exporter> extends ExportOperationHandler<EXPORT, EXPORTER, Object> {
    @Override
    public Object doOperation(final EXPORT export,
                              final Context context,
                              final Store store,
                              final EXPORTER exporter)
            throws OperationException {
        final Iterable<?> inputItr = wrapInIterable(export.getInput());
        exporter.add(export.getKey(), inputItr);
        return export.getInput();
    }

    private Iterable<?> wrapInIterable(final Object input) {
        if (null == input) {
            return null;
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
