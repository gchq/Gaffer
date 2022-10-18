/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.imprt;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.imprt.ImportFrom;
import uk.gov.gchq.gaffer.operation.imprt.Importer;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.Arrays;
import java.util.Collections;

/**
 * Abstract class describing how to handle {@link ImportFrom} operations.
 *
 * @param <IMPORT> the {@link ImportFrom} operation
 * @param <IMPORTER> the {@link Importer} instance
 */
public abstract class ImportFromHandler<IMPORT extends ImportFrom, IMPORTER extends Importer> extends ImportOperationHandler<IMPORT, IMPORTER> {
    @Override
    public Object doOperation(final IMPORT imprt,
                              final Context context,
                              final Store store,
                              final IMPORTER importer)
            throws OperationException {
        return importer.add((String) imprt.getInput());
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
