/*
 * Copyright 2016-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.imprt.Import;
import uk.gov.gchq.gaffer.operation.imprt.Importer;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * Abstract class describing how to handle {@link Import} operations.
 *
 * @param <IMPORT> the {@link Import} operation
 * @param <IMPORTER> the {@link Importer} instance
 */
public abstract class ImportOperationHandler<IMPORT extends Import & Operation, IMPORTER extends Importer> implements OperationHandler<IMPORT> {
    @Override
    public Object doOperation(final IMPORT imprt,
                              final Context context, final Store store)
            throws OperationException {
        IMPORTER importer = context.getImporter(getImporterClass());
        if (null == importer) {
            importer = createImporter(imprt, context, store);
            if (null == importer) {
                throw new OperationException("Unable to create importer: " + getImporterClass());
            }
            context.addImporter(importer);
        }

        return doOperation(imprt, context, store, importer);
    }

    protected abstract Class<IMPORTER> getImporterClass();

    protected abstract IMPORTER createImporter(final IMPORT imprt, final Context context, final Store store);

    protected abstract Object doOperation(final IMPORT imprt, final Context context, final Store store, final IMPORTER importer) throws OperationException;
}
