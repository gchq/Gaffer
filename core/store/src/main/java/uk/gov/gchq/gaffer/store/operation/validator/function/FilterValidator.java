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

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of FunctionValidator, used for validating a Filter function.
 */
public class FilterValidator extends FunctionValidator<Filter> {

    @Override
    protected ValidationResult validateOperation(final Filter operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        final Map<String, ?> entities = null != operation.getEntities() ? operation.getEntities() : new HashMap<>();
        final Map<String, ?> edges = null != operation.getEdges() ? operation.getEdges() : new HashMap<>();

        final Map<String, SchemaEntityDefinition> schemaEntities = schema.getEntities();
        final Map<String, SchemaEdgeDefinition> schemaEdges = schema.getEdges();

        for (final Map.Entry<String, ?> entry : edges.entrySet()) {
            result.add(validateEdge(entry, schema));
            result.add(validateElementFilter((ElementFilter) entry.getValue()));
            result.add(validateFilterPropertyClasses(schemaEdges, (ElementFilter) entry.getValue()));
        }

        for (final Map.Entry<String, ?> entry : entities.entrySet()) {
            result.add(validateEntity(entry, schema));
            result.add(validateElementFilter((ElementFilter) entry.getValue()));
            result.add(validateFilterPropertyClasses(schemaEntities, (ElementFilter) entry.getValue()));
        }

        final ElementFilter globalElements = operation.getGlobalElements();
        final ElementFilter globalEdges = operation.getGlobalEdges();
        final ElementFilter globalEntities = operation.getGlobalEntities();

        if (null != globalElements) {
            result.add(validateFilterPropertyClasses(schemaEdges, globalElements));
            result.add(validateFilterPropertyClasses(schemaEntities, globalElements));
        }
        if (null != globalEdges) {
            result.add(validateFilterPropertyClasses(schemaEdges, globalEdges));
        }
        if (null != globalEntities) {
            result.add(validateFilterPropertyClasses(schemaEntities, globalEntities));
        }


        return result;
    }

    private ValidationResult validateElementFilter(final ElementFilter filter) {
        final ValidationResult result = new ValidationResult();
        if (null != filter && null != filter.getComponents()) {
            for (final TupleAdaptedPredicate<String, ?> adaptedPredicate : filter.getComponents()) {
                if (null == adaptedPredicate.getPredicate()) {
                    result.addError(filter.getClass().getSimpleName() + " contains a null function.");
                }
            }
        }
        return result;
    }

    /**
     * Validates that the predicates to be executed are assignable to the corresponding properties
     * @param elements  Map of element group to SchemaElementDefinition
     * @param filter    The ElementFilter to be validated against
     * @return          ValidationResult of the validation
     */
    private ValidationResult validateFilterPropertyClasses(final Map<String, ? extends SchemaElementDefinition> elements, final ElementFilter filter) {
        final ValidationResult result = new ValidationResult();

        final List<TupleAdaptedPredicate<String, ?>> components = filter.getComponents();
        for (final TupleAdaptedPredicate<String, ?> component : components) {
            final String[] selection = component.getSelection();

            for (final SchemaElementDefinition elementDef : elements.values()) {
                final Class[] selectionClasses = Arrays.stream(selection).map(elementDef::getPropertyClass).toArray(Class[]::new);
                final Map<String, String> properties = elementDef.getPropertyMap();
                if (!properties.isEmpty()) {
                    if (null == component.getPredicate()) {
                        result.addError(filter.getClass().getSimpleName() + " contains a null function.");
                    } else {
                        final Signature inputSig = Signature.getInputSignature(component.getPredicate());
                        result.add(inputSig.assignable(selectionClasses));
                    }
                }
            }
        }
        return result;
    }
}
