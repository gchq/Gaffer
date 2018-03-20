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
package uk.gov.gchq.gaffer.store.operation.validator.function;

import org.apache.commons.lang3.ArrayUtils;

import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of FunctionValidator, used for validating a Transform function.
 */
public class TransformValidator extends FunctionValidator<Transform> {

    @Override
    protected ValidationResult validateOperation(final Transform operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        final Map<String, ?> entities = null != operation.getEntities() ? operation.getEntities() : new HashMap<>();
        final Map<String, ?> edges = null != operation.getEdges() ? operation.getEdges() : new HashMap<>();

        for (final Map.Entry<String, ?> entry : edges.entrySet()) {
            result.add(validateEdge(entry, schema));
            result.add(validateElementTransformer((ElementTransformer) entry.getValue()));
            result.add(validateTransformPropertyClasses(schema.getEdge(entry.getKey()), (ElementTransformer) entry.getValue()));
        }

        for (final Map.Entry<String, ?> entry : entities.entrySet()) {
            result.add(validateEntity(entry, schema));
            result.add(validateElementTransformer((ElementTransformer) entry.getValue()));
            result.add(validateTransformPropertyClasses(schema.getEntity(entry.getKey()), (ElementTransformer) entry.getValue()));
        }

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

    /**
     * Validates that the functions to be executed are assignable to the corresponding properties
     *
     * @param elementDef  The SchemaElementDefinition to validate against.
     * @param transformer The ElementFilter to be validated against
     * @return ValidationResult of the validation
     */
    private ValidationResult validateTransformPropertyClasses(final SchemaElementDefinition elementDef, final ElementTransformer transformer) {
        final ValidationResult result = new ValidationResult();

        if (null != elementDef) {
            final List<TupleAdaptedFunction<String, ?, ?>> components = transformer.getComponents();
            for (final TupleAdaptedFunction<String, ?, ?> component : components) {
                final Map<String, String> properties = elementDef.getPropertyMap();
                if (!properties.isEmpty()) {
                    if (null == component.getFunction()) {
                        result.addError(transformer.getClass().getSimpleName());
                    } else {
                        final Class[] selectionClasses = getTypeClasses(component.getSelection(), elementDef);
                        if (!ArrayUtils.contains(selectionClasses, null)) {
                            final Signature inputSig = Signature.getInputSignature(component.getFunction());
                            result.add(inputSig.assignable(selectionClasses));
                        }
                        final Class[] projectionClasses = getTypeClasses(component.getProjection(), elementDef);
                        if (!ArrayUtils.contains(projectionClasses, null)) {
                            final Signature outputSig = Signature.getOutputSignature(component.getFunction());
                            result.add(outputSig.assignable(projectionClasses));
                        }
                    }
                }
            }
        }
        return result;
    }
}
