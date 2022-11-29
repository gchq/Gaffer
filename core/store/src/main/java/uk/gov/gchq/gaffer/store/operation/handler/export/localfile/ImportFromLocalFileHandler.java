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

package uk.gov.gchq.gaffer.store.operation.handler.export.localfile;

import uk.gov.gchq.gaffer.operation.OperationException;

import uk.gov.gchq.gaffer.operation.impl.export.localfile.ImportFromLocalFile;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.LocalFileExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.export.GetExportHandler;

/**
 * Implementation of the {@link GetExportHandler} to retrieve exported created by
 * a {@link LocalFileExporter}.
 */
public class ImportFromLocalFileHandler extends GetExportHandler<ImportFromLocalFile, LocalFileExporter> {
    @Override
    protected Iterable<?> getExport(final ImportFromLocalFile imprt, final LocalFileExporter exporter) throws OperationException {
        return exporter.get(imprt.getKeyOrDefault());
    }

    @Override
    protected Class<LocalFileExporter> getExporterClass() {
        return LocalFileExporter.class;
    }

    @Override
    public LocalFileExporter createExporter(final ImportFromLocalFile imprt, final Context context, final Store store) {
        return new LocalFileExporter();
    }
}
