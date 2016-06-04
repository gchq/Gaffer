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

package gaffer.store.operation.handler;

import gaffer.export.Exporter;
import gaffer.operation.OperationException;
import gaffer.operation.impl.export.ExportOperation;
import gaffer.operation.impl.export.FetchExport;
import gaffer.operation.impl.export.FetchExportResult;
import gaffer.operation.impl.export.UpdateExport;
import gaffer.operation.impl.export.initialise.InitialiseExport;
import gaffer.store.Store;
import gaffer.store.export.ExportHolder;
import gaffer.user.User;

public class ExportHandler implements OperationHandler<ExportOperation<?, Object>, Object> {
    @Override
    public Object doOperation(final ExportOperation operation,
                              final User user, final Store store)
            throws OperationException {
        throw new UnsupportedOperationException("Export handlers are special and require an extra ExportHolder parameter.");
    }

    public Object doOperation(final ExportOperation op,
                              final User user, final Store store, final ExportHolder exportHolder)
            throws OperationException {
        final Object result;
        if (op instanceof InitialiseExport) {
            result = initialiseExport((InitialiseExport) op, user, store, exportHolder);
        } else {
            final Exporter exporter = exportHolder.get();
            if (op instanceof UpdateExport) {
                result = updateExport((UpdateExport) op, user, exporter);
            } else if (op instanceof FetchExport) {
                result = exporter;
            } else if (op instanceof FetchExportResult) {
                result = fetchExportResult((FetchExportResult) op, user, exporter);
            } else {
                throw new UnsupportedOperationException("Export operation is not supported: " + op.getClass().getName());
            }
        }

        return result;
    }

    private Object initialiseExport(final InitialiseExport initExport, final User user, final Store store, final ExportHolder exportHolder) {
        final Exporter exporter = initExport.getExporter();
        exporter.initialise(store, user);
        exportHolder.set(exporter);
        return initExport.getInput();
    }

    private Iterable<?> updateExport(final UpdateExport updateExport, final User user, final Exporter exporter) {
        exporter.add(updateExport.getKey(), updateExport.getInput(), user);
        return exporter.get(updateExport.getKey(), user, 0, Integer.MAX_VALUE);
    }

    private Object fetchExportResult(final FetchExportResult op, final User user, final Exporter exporter) {
        return exporter.get(op.getKey(), user, op.getStart(), op.getEnd());
    }
}
