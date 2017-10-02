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
import uk.gov.gchq.gaffer.operation.impl.function.Function;
import uk.gov.gchq.gaffer.operation.util.AggregatePair;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.binaryoperator.AdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.ArrayList;
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

        result.add(validateEdges(operation, schema));
        result.add(validateEntities(operation, schema));

        return result;
    }

    private ValidationResult validateEntities(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();

        if (null != operation.getEntities()) {
            for (final Map.Entry<String, ?> entry : operation.getEntities().entrySet()) {

                final String group = entry.getKey();
                final SchemaEntityDefinition schemaEntityDefinition = schema.getEntity(group);
                if (null == schemaEntityDefinition) {
                    result.addError("Entity group: " + group + " does not exist in the schema.");
                }

                result.add(validateFunction(entry, schema));
            }
        }

        return result;
    }

    private ValidationResult validateEdges(final T operation, final Schema schema) {
        final ValidationResult result = new ValidationResult();

        if (null != operation.getEdges()) {
            for (final Map.Entry<String, ?> entry : operation.getEdges().entrySet()) {

                final String group = entry.getKey();
                final SchemaEdgeDefinition schemaEdgeDefinition = schema.getEdge(group);
                if (null == schemaEdgeDefinition) {
                    result.addError("Edge group: " + group + " does not exist in the schema.");
                }

                result.add(validateFunction(entry, schema));
            }
        }

        return result;
    }

    private ValidationResult validateFunction(final Map.Entry<String, ?> entry, final Schema schema) {
        final ValidationResult result = new ValidationResult();

        if (entry.getValue() instanceof AggregatePair) {
            result.add(validateAggregator(entry, schema));
        }

        if (entry.getValue() instanceof ElementFilter) {
            final ElementFilter filter = (ElementFilter) entry.getValue();
            if (null != filter && null != filter.getComponents()) {
                for (final TupleAdaptedPredicate<String, ?> adaptedPredicate : filter.getComponents()) {
                    if (null == adaptedPredicate.getPredicate()) {
                        result.addError(filter.getClass().getSimpleName() + " contains a null function.");
                    }
                }
            }
        }

        if (entry.getValue() instanceof ElementTransformer) {
            final ElementTransformer transformer = (ElementTransformer) entry.getValue();
            if (null != transformer && null != transformer.getComponents()) {
                for (final TupleAdaptedFunction<String, ?, ?> adaptedTransformer : transformer.getComponents()) {
                    if (null == adaptedTransformer.getFunction()) {
                        result.addError(transformer.getClass().getSimpleName() + " contains a null function.");
                    }
                }
            }
        }
        return result;
    }

    private ValidationResult validateAggregator(final Map.Entry<String, ?> entry, final Schema schema) {
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
}
