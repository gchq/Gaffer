/*
 * Copyright 2016 Crown Copyright
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

import uk.gov.gchq.gaffer.export.Exporter;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * An <code>FetchExporterHandler</code> handles {@link FetchExporter} operations.
 * Simply returns the exporter with the provided export key.
 */
public class FetchExporterHandler implements OperationHandler<FetchExporter, Exporter> {
    @Override
    public Exporter doOperation(final FetchExporter fetchExporter,
                                final Context context, final Store store)
            throws OperationException {
        final Exporter exporter = context.getExporter(fetchExporter.getKey());
        if (null == exporter) {
            throw new IllegalArgumentException("No exporter was found for export key: " + fetchExporter.getKey());
        }

        return exporter;
    }
}
