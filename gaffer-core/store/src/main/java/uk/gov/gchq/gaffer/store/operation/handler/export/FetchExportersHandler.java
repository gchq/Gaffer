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
import uk.gov.gchq.gaffer.operation.impl.export.FetchExporters;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import java.util.Map;

/**
 * An <code>FetchExportersHandler</code> handles {@link FetchExporters} operations.
 * Simply returns the {@link Map} of {@link Exporter}s.
 */
public class FetchExportersHandler implements OperationHandler<FetchExporters, Map<String, Exporter>> {
    @Override
    public Map<String, Exporter> doOperation(final FetchExporters fetchExporters,
                                             final Context context, final Store store)
            throws OperationException {
        return context.getExporters();
    }
}
