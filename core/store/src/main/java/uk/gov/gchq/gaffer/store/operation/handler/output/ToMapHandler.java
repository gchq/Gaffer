/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.output;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Map;

/**
 * A {@code ToMapHandler} handles {@link ToMap} operations by applying the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to each item in the
 * input {@link Iterable}.
 */
public class ToMapHandler implements OutputOperationHandler<ToMap, Iterable<? extends Map<String, Object>>> {
    @Override
    public Iterable<? extends Map<String, Object>> doOperation(final ToMap operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        return operation.getElementGenerator().apply(operation.getInput());
    }
}
