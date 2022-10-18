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

package uk.gov.gchq.gaffer.store.operation.handler.imprt.localfile;

import uk.gov.gchq.gaffer.operation.impl.imprt.localfile.ImportFromLocalFile;
import uk.gov.gchq.gaffer.operation.impl.imprt.localfile.LocalFileImporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.imprt.ImportFromHandler;

public class ImportFromLocalFileHandler extends ImportFromHandler<ImportFromLocalFile, LocalFileImporter> {
    @Override
    protected Class<LocalFileImporter> getImporterClass() {
        return LocalFileImporter.class;
    }

    @Override
    protected LocalFileImporter createImporter(final ImportFromLocalFile imprt, final Context context, final Store store) {
        return new LocalFileImporter();
    }
}

