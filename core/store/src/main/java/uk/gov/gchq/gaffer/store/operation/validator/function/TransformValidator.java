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
package uk.gov.gchq.gaffer.store.operation.validator.function;

import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformValidator extends FunctionValidator<Transform> {

    protected ValidationResult validateOperation(final Transform operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        final Map<String, ?> entities = null != operation.getEntities() ? operation.getEntities() : new HashMap<>();
        final Map<String, ?> edges = null != operation.getEdges() ? operation.getEdges() : new HashMap<>();

        final Map<String, SchemaEntityDefinition> schemaEntities = schema.getEntities();
        final Map<String, SchemaEdgeDefinition> schemaEdges = schema.getEdges();

        edges.forEach((key, value) -> result.add(validateElementTransformer((ElementTransformer) value)));
        edges.forEach((key, value) -> result.add(validateTransformPropertyClasses(schemaEdges, (ElementTransformer) value)));

        entities.forEach((key, value) -> result.add(validateElementTransformer((ElementTransformer) value)));
        entities.forEach((key, value) -> result.add(validateTransformPropertyClasses(schemaEntities, (ElementTransformer) value)));

        return result;
    }

    private ValidationResult validateElementTransformer(final ElementTransformer transformer) {
        final ValidationResult result = new ValidationResult();
        if (null != transformer && null != transformer.getComponents()) {
            for (final TupleAdaptedFunction<String, ?, ?> adaptedTransformer : transformer.getComponents()) {
                if (null == adaptedTransformer.getFunction()) {
                    result.addError(transformer.getClass().getSimpleName() + " contains a null function.");
                }
            }
        }
        return result;
    }

    private ValidationResult validateTransformPropertyClasses(final Map<String, ? extends SchemaElementDefinition> elements, final ElementTransformer transformer) {
        final ValidationResult result = new ValidationResult();

        final List<TupleAdaptedFunction<String, ?, ?>> components = transformer.getComponents();
        for (final TupleAdaptedFunction<String, ?, ?> component : components) {
            final String[] selection = component.getSelection();
            final String[] projection = component.getProjection();

            for (final SchemaElementDefinition elementDef : elements.values()) {
                final Class[] selectionClasses = Arrays.stream(selection).map(elementDef::getPropertyClass).toArray(Class[]::new);
                final Class[] projectionClasses = Arrays.stream(projection).map(elementDef::getPropertyClass).toArray(Class[]::new);
                final Map<String, String> properties = elementDef.getPropertyMap();
                if (!properties.isEmpty()) {
                    if (null == component.getFunction()) {
                        result.addError(transformer.getClass().getSimpleName());
                    } else {
                        final Signature inputSig = Signature.getInputSignature(component.getFunction());
                        result.add(inputSig.assignable(selectionClasses));

                        final Signature outputSig = Signature.getOutputSignature(component.getFunction());
                        result.add(outputSig.assignable(projectionClasses));
                    }
                }
            }
        }
        return result;
    }
}
