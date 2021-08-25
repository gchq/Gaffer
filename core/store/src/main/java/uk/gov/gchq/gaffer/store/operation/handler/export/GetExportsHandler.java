/*
 * Copyright 2016-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.GetExport;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handler for GetExports operations.
 */
public class GetExportsHandler implements OperationHandler< Map<String, CloseableIterable<?>>> {
    @Override
    public Map<String, CloseableIterable<?>> _doOperation(final Operation getExports, final Context context, final Store store) throws OperationException {
        final Map<String, CloseableIterable<?>> exports = new LinkedHashMap<>();
        for (final GetExport getExport : getExports.getGetExports()) {
            final CloseableIterable<?> export = (CloseableIterable<?>) store.execute(new OperationChain((Operation) getExport), context);
            exports.put(getExport.getClass().getName() + ": " + getExport.getKeyOrDefault(), export);
        }

        return exports;
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return null;
    }
}
