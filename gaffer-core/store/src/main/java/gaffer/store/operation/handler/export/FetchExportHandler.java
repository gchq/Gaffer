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

package gaffer.store.operation.handler.export;

import gaffer.export.Exporter;
import gaffer.operation.OperationException;
import gaffer.operation.impl.export.FetchExport;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import java.util.Collections;

/**
 * An <code>FetchExportHandler</code> handles {@link FetchExport} operations.
 * Returns the exported objects with the provided export key.
 * Only a single page of the export objects are returned. This is controlled
 * with the start and end positions provided in the fetch export operation.
 */
public class FetchExportHandler implements OperationHandler<FetchExport, Iterable<?>> {
    @Override
    public Iterable<?> doOperation(final FetchExport fetchExport,
                                   final Context context, final Store store)
            throws OperationException {

        final Exporter exporter = context.getExporter(fetchExport.getKey());

        final Iterable<?> result;
        if (null != exporter) {
            result = exporter.get(context.getUser(), fetchExport.getStart(), fetchExport.getEnd());
        } else {
            result = Collections.emptySet();
        }

        return result;
    }
}
