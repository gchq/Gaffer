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

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.binaryoperator.AdaptedBinaryOperator;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * An implementation of FunctionValidator, used for validating an Aggregate function.
 */
public class AggregateValidator extends FunctionValidator<Aggregate> {

    @Override
    protected ValidationResult validateOperation(final Aggregate operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();
        final Map<String, ?> entities = null != operation.getEntities() ? operation.getEntities() : new HashMap<>();
        final Map<String, ?> edges = null != operation.getEdges() ? operation.getEdges() : new HashMap<>();

        final Map<String, SchemaEntityDefinition> schemaEntities = schema.getEntities();
        final Map<String, SchemaEdgeDefinition> schemaEdges = schema.getEdges();

        for (final Map.Entry<String, ?> entry : edges.entrySet()) {
            result.add(validateEdge(entry, schema));
            result.add(validateElementAggregator(entry, schema));
            result.add(validateAggregatePropertyClasses(schemaEdges, (AggregatePair) entry.getValue()));
        }

        for (final Map.Entry<String, ?> entry : entities.entrySet()) {
            result.add(validateEntity(entry, schema));
            result.add(validateElementAggregator(entry, schema));
            result.add(validateAggregatePropertyClasses(schemaEntities, (AggregatePair) entry.getValue()));
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
                if (null == adaptedFunction.getBinaryOperator()) {
                    result.addError(aggregator.getClass().getSimpleName() + " contains a null function.");
                }
            }
        }
        return result;
    }

    /**
     * Validates that the binary operators to be executed are assignable to the corresponding non-transient properties
     * @param elements  Map of element group to SchemaElementDefinition
     * @param pair      AggregatePair, containing a String array of groupBy properties, and an ElementAggregator
     * @return          ValidationResult of the validation
     */
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
                        }

                        for (int i = 0; i < selectionClasses.length; i++) {
                            if (properties.containsKey(selection[i])) {
                                final Signature inputSig = Signature.getInputSignature(component.getBinaryOperator());
                                result.add(inputSig.assignable(selectionClasses[i]));
                            }
                        }
                    }
                }
            }
        }

        return result;
    }
}
