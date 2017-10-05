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

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Function;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.binaryoperator.AdaptedBinaryOperator;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class FunctionValidator<T extends Function> {
    public ValidationResult validate(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null == operation) {
            result.addError("Operation cannot be null.");
        }
        if (null == schema) {
            result.addError("Schema cannot be null.");
        }

        result.add(validateOperation(operation, schema));

        return result;
    }

    private ValidationResult validateOperation(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        final boolean opIsAggregate = operation instanceof Aggregate;
        final boolean opIsFilter = operation instanceof Filter;
        final boolean opIsTransform = operation instanceof Transform;
        final Map<String, ?> entities = null != operation.getEntities() ? operation.getEntities() : new HashMap<>();
        final Map<String, ?> edges = null != operation.getEdges() ? operation.getEdges() : new HashMap<>();

        final Map<String, SchemaEntityDefinition> schemaEntities = schema.getEntities();
        final Map<String, SchemaEdgeDefinition> schemaEdges = schema.getEdges();

        entities.entrySet().forEach(e -> result.add(validateEntity(e, schema)));
        edges.entrySet().forEach(e -> result.add(validateEdge(e, schema)));

        if (opIsAggregate) {
            edges.entrySet().forEach(e -> result.add(validateElementAggregator(e, schema)));
            edges.forEach((key, value) -> result.add(validateAggregatePropertyClasses(schemaEdges, (AggregatePair) value)));

            entities.entrySet().forEach(e -> result.add(validateElementAggregator(e, schema)));
            entities.forEach((key, value) -> result.add(validateAggregatePropertyClasses(schemaEntities, (AggregatePair) value)));
        }

        if (opIsFilter) {
            edges.forEach((key, value) -> result.add(validateElementFilter((ElementFilter) value)));
            edges.forEach((key, value) -> result.add(validateFilterPropertyClasses(schemaEdges, (ElementFilter) value)));

            entities.forEach((key, value) -> result.add(validateElementFilter((ElementFilter) value)));
            entities.forEach((key, value) -> result.add(validateFilterPropertyClasses(schemaEntities, (ElementFilter) value)));

            final ElementFilter globalElements = ((Filter) operation).getGlobalElements();
            final ElementFilter globalEdges = ((Filter) operation).getGlobalEdges();
            final ElementFilter globalEntities = ((Filter) operation).getGlobalEntities();

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
        }

        if (opIsTransform) {
            edges.forEach((key, value) -> result.add(validateElementTransformer((ElementTransformer) value)));
            edges.forEach((key, value) -> result.add(validateTransformPropertyClasses(schemaEdges, (ElementTransformer) value)));

            entities.forEach((key, value) -> result.add(validateElementTransformer((ElementTransformer) value)));
            entities.forEach((key, value) -> result.add(validateTransformPropertyClasses(schemaEntities, (ElementTransformer) value)));
        }

        return result;
    }

    private ValidationResult validateEdge(final Map.Entry<String, ?> edgeEntry, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null != edgeEntry) {
            final String group = edgeEntry.getKey();
            final SchemaEdgeDefinition schemaEdgeDefinition = schema.getEdge(group);
            if (null == schemaEdgeDefinition) {
                result.addError("Edge group: " + group + " does not exist in the schema.");
            }
        }
        return result;
    }

    private ValidationResult validateEntity(final Map.Entry<String, ?> entityEntry, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        if (null != entityEntry) {
            final String group = entityEntry.getKey();
            final SchemaEntityDefinition schemaEntityDefinition = schema.getEntity(group);
            if (null == schemaEntityDefinition) {
                result.addError("Entity group: " + group + " does not exist in the schema.");
            }
        }
        return result;
    }

    private ValidationResult validateElementAggregator(final Map.Entry<String, ?> entry, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        List<TupleAdaptedBinaryOperator<String, ?>> aggregateFunctions = new ArrayList<>();
        final SchemaElementDefinition schemaElement = schema.getElement(entry.getKey());

        if (null != schemaElement) {
            aggregateFunctions = schemaElement.getOriginalAggregateFunctions();
        }

        List<BinaryOperator<?>> schemaOperators = new ArrayList<>();

        if (null != aggregateFunctions) {
            schemaOperators = Streams.toStream(aggregateFunctions)
                    .map(AdaptedBinaryOperator::getBinaryOperator)
                    .collect(Collectors.toList());
        }

        if (schemaOperators.contains(null)) {
            result.addError("Schema contains an ElementAggregator with a null function.");
        }

        final AggregatePair pair = (AggregatePair) entry.getValue();
        final ElementAggregator aggregator = pair.getElementAggregator();

        if (null != aggregator && null != aggregator.getComponents()) {
            for (final TupleAdaptedBinaryOperator<String, ?> adaptedFunction : aggregator.getComponents()) {
                if (null != adaptedFunction.getBinaryOperator()) {
                    if (null != schemaElement &&
                            null != schemaElement.getOriginalAggregateFunctions() &&
                            !schemaOperators.contains(adaptedFunction.getBinaryOperator())) {
                        result.addError(adaptedFunction.getBinaryOperator().toString() + " does not exist in the schema.");
                    }
                } else {
                    result.addError(aggregator.getClass().getSimpleName() + " contains a null function.");
                }
            }
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

    private ValidationResult validateAggregatePropertyClasses(final Map<String, ? extends SchemaElementDefinition> elements, final AggregatePair pair) {
        final ValidationResult result = new ValidationResult();

        final ElementAggregator aggregator = pair.getElementAggregator();
        if (null != aggregator) {
            final List<TupleAdaptedBinaryOperator<String, ?>> components = aggregator.getComponents();

            for (final TupleAdaptedBinaryOperator<String, ?> component : components) {
                final String[] selection = component.getSelection();

                for (final SchemaElementDefinition elementDef : elements.values()) {
                    final Class[] selectionClasses = Arrays.stream(selection).map(elementDef::getPropertyClass).toArray(Class[]::new);
                    final Map<String, String> properties = elementDef.getPropertyMap();
                    if (!properties.isEmpty()) {
                        if (null == component.getBinaryOperator()) {
                            result.addError(aggregator.getClass().getSimpleName() + " contains a null function.");
                        } else {
                            final Signature inputSig = Signature.getInputSignature(component.getBinaryOperator());
                            result.add(inputSig.assignable(selectionClasses));
                        }
                    }
                }
            }
        } else {
            for (final SchemaElementDefinition elementDef : elements.values()) {
                final List<TupleAdaptedBinaryOperator<String, ?>> components = elementDef.getOriginalAggregateFunctions();
                for (final TupleAdaptedBinaryOperator<String, ?> component : components) {
                    final String[] selection = component.getSelection();
                    final Class[] selectionClasses = Arrays.stream(selection).map(elementDef::getPropertyClass).toArray(Class[]::new);
                    final Map<String, String> properties = elementDef.getPropertyMap();
                    if (!properties.isEmpty()) {
                        if (null == component.getBinaryOperator()) {
                            result.addError("Aggregator in the schema contains a null function.");
                        } else {
                            final Signature inputSig = Signature.getInputSignature(component.getBinaryOperator());
                            result.add(inputSig.assignable(selectionClasses));
                        }
                    }
                }
            }
        }

        return result;
    }

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
