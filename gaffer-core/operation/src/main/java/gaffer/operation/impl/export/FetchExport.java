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

package gaffer.operation.impl.export;

import gaffer.export.Exporter;
import gaffer.operation.AbstractOperation;

/**
 * A <code>FetchExport</code> fetches the {@link Exporter} containing the export
 * information.
 *
 * @see UpdateExport
 * @see FetchExportResult
 */
public class FetchExport extends AbstractOperation<Void, Exporter> implements ExportOperation<Void, Exporter> {
    public static class Builder extends AbstractOperation.Builder<FetchExport, Void, Exporter> {
        public Builder() {
            super(new FetchExport());
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
