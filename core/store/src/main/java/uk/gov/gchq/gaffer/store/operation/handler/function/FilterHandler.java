/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.function;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.util.StreamFilterIterable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.validator.function.FilterValidator;
import uk.gov.gchq.gaffer.store.operation.validator.function.FunctionValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.HashMap;
import java.util.Map;

public class FilterHandler implements OutputOperationHandler<Filter, Iterable<? extends Element>> {

    private final FunctionValidator<Filter> validator = new FilterValidator();

    @Override
    public Iterable<? extends Element> doOperation(final Filter operation, final Context context, final Store store) throws OperationException {
        return doOperation(operation, store.getSchema());
    }

    public Iterable<? extends Element> doOperation(final Filter operation, final Schema schema) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Filter operation has null iterable of elements");
        }

        // If no entities or edges have been provided then we will assume
        // all elements should be used. This matches the way a View works.
        if (null == operation.getEntities() && null == operation.getEdges()) {
            final Map<String, ElementFilter> entityMap = new HashMap<>();
            schema.getEntityGroups().forEach(e -> entityMap.put(e, new ElementFilter()));
            operation.setEntities(entityMap);

            final Map<String, ElementFilter> edgeMap = new HashMap<>();
            schema.getEdgeGroups().forEach(e -> edgeMap.put(e, new ElementFilter()));
            operation.setEdges(edgeMap);
        }

        final ValidationResult result = validator.validate(operation, schema);
        if (!result.isValid()) {
            throw new OperationException("Filter operation is invalid. " + result.getErrorString());
        }
        return new StreamFilterIterable(operation);
    }
}
