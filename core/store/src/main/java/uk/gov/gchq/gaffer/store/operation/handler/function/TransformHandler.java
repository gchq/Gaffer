/*
 * Copyright 2017-2020 Crown Copyright
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
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.util.StreamTransformIterable;
import uk.gov.gchq.gaffer.store.operation.validator.function.FunctionValidator;
import uk.gov.gchq.gaffer.store.operation.validator.function.TransformValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.HashMap;
import java.util.Map;

public class TransformHandler implements OperationHandler<Iterable<? extends Element>> {
    public static final String KEY_EDGE_TRANSFORMER = "edgeTransformer";
    public static final String KEY_ENTITY_TRANSFORMER = "entityTransformer";
    private final FunctionValidator validator = new TransformValidator();

    @Override
    public Iterable<? extends Element> _doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        return doOperation(operation, store.getSchema());
    }

    public Iterable<? extends Element> doOperation(final Operation operation, final Schema schema) throws OperationException {
        if (null == operation.input()) {
            throw new OperationException("Transform operation has null iterable of elements");
        }

        // If no entities or edges have been provided then we will assume
        // all elements should be used. This matches the way a View works.
        if (null == operation.get(KEY_ENTITY_TRANSFORMER) && null == operation.get(KEY_EDGE_TRANSFORMER)) {
            final Map<String, ElementTransformer> entityMap = new HashMap<>();
            schema.getEntityGroups().forEach(e -> entityMap.put(e, new ElementTransformer()));
            operation.operationArg(KEY_ENTITY_TRANSFORMER, entityMap);

            final Map<String, ElementTransformer> edgeMap = new HashMap<>();
            schema.getEdgeGroups().forEach(e -> edgeMap.put(e, new ElementTransformer()));
            operation.operationArg(KEY_EDGE_TRANSFORMER, edgeMap);
        }

        final ValidationResult result = validator.validate(operation, schema);
        if (!result.isValid()) {
            throw new OperationException("Transform operation is invalid. " + result.getErrorString());
        }

        return new StreamTransformIterable(operation);
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .fieldOptional(KEY_EDGE_TRANSFORMER, Map.class)
                .fieldOptional(KEY_ENTITY_TRANSFORMER, Map.class);
    }

    static class Builder extends BuilderSpecificInputOperation<Builder> {

        public Builder edgeTransformer(final String group, final ElementTransformer elementTransformer) {
            HashMap<String, ElementTransformer> transformerHashMap = (HashMap<String, ElementTransformer>) operation.getOrDefault(KEY_EDGE_TRANSFORMER, new HashMap<String, ElementTransformer>());
            transformerHashMap.put(group, elementTransformer);

            edgeTransformer(transformerHashMap);
            return this;
        }

        public Builder edgeTransformer(Map<String, ElementTransformer> transformer) {
            operationArg(KEY_EDGE_TRANSFORMER, transformer);
            return this;
        }

        public Builder entityTransformer(final String group, final ElementTransformer elementTransformer) {
            HashMap<String, ElementTransformer> transformerHashMap = (HashMap<String, ElementTransformer>) operation.getOrDefault(KEY_ENTITY_TRANSFORMER, new HashMap<String, ElementTransformer>());
            transformerHashMap.put(group, elementTransformer);

            entityTransformer(transformerHashMap);
            return this;
        }

        public Builder entityTransformer(Map<String, ElementTransformer> transformer) {
            operationArg(KEY_ENTITY_TRANSFORMER, transformer);
            return this;
        }

        @Override
        protected Builder getBuilder() {
            return this;
        }

        @Override
        protected FieldDeclaration getFieldDeclaration() {
            return new TransformHandler().getFieldDeclaration();
        }
    }
}
