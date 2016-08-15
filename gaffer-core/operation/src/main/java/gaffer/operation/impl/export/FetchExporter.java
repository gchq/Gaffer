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
import gaffer.operation.VoidInput;

/**
 * A <code>FetchExporter</code> fetches the {@link Exporter} containing the export
 * information.
 *
 * @see UpdateExport
 * @see FetchExport
 */
public class FetchExporter extends ExportOperation<Void, Exporter> implements VoidInput<Exporter> {
    public FetchExporter() {
        super();
    }

    public FetchExporter(final String key) {
        super(key);
    }

    public static class Builder extends ExportOperation.Builder<FetchExporter, Void, Exporter> {
        public Builder() {
            super(new FetchExporter());
        }
    }
}
