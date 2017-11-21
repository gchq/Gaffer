/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.store.schema;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.composite.Composite;
import uk.gov.gchq.koryphe.signature.Signature;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * An {@code ViewValidator} validates a view against a {@link Schema}
 * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition}.
 * Checks all function input and output types are compatible with the
 * properties and identifiers in the Schema and the transient properties in the
 * View.
 */
public class ViewValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewValidator.class);

    /**
     * Checks all {@link java.util.function.Predicate}s and {@link java.util.function.Function}s defined are
     * compatible with the identifiers and properties in the {@link Schema}
     * and transient properties in the {@link View}.
     *
     * @param view        the {@link View} to validate
     * @param schema      the {@link Schema} to validate the view against
     * @param storeTraits the store traits
     * @return true if the element definition is valid, otherwise false and an error is logged
     */
    public ValidationResult validate(final View view, final Schema schema, final Set<StoreTrait> storeTraits) {
        final boolean isStoreOrdered = storeTraits.contains(StoreTrait.ORDERED);

        final ValidationResult result = new ValidationResult();

        if (null != view) {
            if (null != view.getEntities()) {
                for (final Map.Entry<String, ViewElementDefinition> entry : view.getEntities().entrySet()) {
                    final String group = entry.getKey();
                    final SchemaEntityDefinition schemaElDef = schema.getEntity(group);
                    final ViewElementDefinition viewElDef = entry.getValue();

                    if (null == schemaElDef) {
                        result.addError("Entity group " + group + " does not exist in the schema");
                    } else {
                        for (final String transProp : viewElDef.getTransientProperties()) {
                            if (schemaElDef.containsProperty(transProp)) {
                                result.addError("Transient property " + transProp + " for entity group " + group + " is not transient as it has been found in the schema");
                            }
                        }

                        result.add(validateAgainstStoreTraits(viewElDef, storeTraits));
                        result.add(validateFunctionArgumentTypes(viewElDef.getPreAggregationFilter(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getAggregator(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getPostAggregationFilter(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getTransformer(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getPostTransformFilter(), viewElDef, schemaElDef));
                        result.add(validateGroupBy(isStoreOrdered, group, viewElDef, schemaElDef));
                    }
                }
            }

            if (null != view.getEdges()) {
                for (final Map.Entry<String, ViewElementDefinition> entry : view.getEdges().entrySet()) {
                    final String group = entry.getKey();
                    final SchemaEdgeDefinition schemaElDef = schema.getEdge(group);
                    final ViewElementDefinition viewElDef = entry.getValue();

                    if (null == schemaElDef) {
                        result.addError("Edge group " + group + " does not exist in the schema");
                    } else {
                        for (final String transProp : viewElDef.getTransientProperties()) {
                            if (schemaElDef.containsProperty(transProp)) {
                                result.addError("Transient property " + transProp + " for edge group " + group + " is not transient as it has been found in the schema");
                            }
                        }

                        result.add(validateAgainstStoreTraits(viewElDef, storeTraits));
                        result.add(validateFunctionArgumentTypes(viewElDef.getPreAggregationFilter(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getAggregator(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getPostAggregationFilter(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getTransformer(), viewElDef, schemaElDef));
                        result.add(validateFunctionArgumentTypes(viewElDef.getPostTransformFilter(), viewElDef, schemaElDef));
                        result.add(validateGroupBy(isStoreOrdered, group, viewElDef, schemaElDef));
                    }
                }
            }
        }

        return result;
    }

    protected ValidationResult validateAgainstStoreTraits(final ViewElementDefinition viewElDef, final Set<StoreTrait> storeTraits) {
        final ValidationResult result = new ValidationResult();

        if (!storeTraits.contains(StoreTrait.QUERY_AGGREGATION) && null != viewElDef.getAggregator()) {
            result.addError("This store does not currently support " + StoreTrait.QUERY_AGGREGATION.name());
        }
        validateStoreTrait(viewElDef.getPreAggregationFilter(), StoreTrait.PRE_AGGREGATION_FILTERING, storeTraits, result);
        validateStoreTrait(viewElDef.getPostAggregationFilter(), StoreTrait.POST_AGGREGATION_FILTERING, storeTraits, result);
        validateStoreTrait(viewElDef.getTransformer(), StoreTrait.TRANSFORMATION, storeTraits, result);
        validateStoreTrait(viewElDef.getPostTransformFilter(), StoreTrait.POST_TRANSFORMATION_FILTERING, storeTraits, result);

        return result;
    }

    protected ValidationResult validateGroupBy(final boolean isStoreOrdered, final String group, final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final ValidationResult result = new ValidationResult();
        final Set<String> viewGroupBy = viewElDef.getGroupBy();
        if (null != viewGroupBy && !viewGroupBy.isEmpty()) {
            final Set<String> schemaGroupBy = schemaElDef.getGroupBy();
            if (null != schemaGroupBy && schemaGroupBy.containsAll(viewGroupBy)) {
                if (isStoreOrdered) {
                    final LinkedHashSet<String> schemaGroupBySubset = Sets.newLinkedHashSet(Iterables.limit(schemaGroupBy, viewGroupBy.size()));
                    if (!viewGroupBy.equals(schemaGroupBySubset)) {
                        result.addError("Group by properties for group " + group + " are not in the same order as the group by properties in the schema. View groupBy:" + viewGroupBy + ". Schema groupBy:" + schemaGroupBy);
                    }
                }
            } else {
                result.addError("Group by properties for group " + group + " in the view are not all included in the group by field in the schema. View groupBy:" + viewGroupBy + ". Schema groupBy:" + schemaGroupBy);
            }
        }

        return result;
    }

    private void validateStoreTrait(final Composite functions, final StoreTrait storeTrait, final Set<StoreTrait> storeTraits, final ValidationResult result) {
        if (!storeTraits.contains(storeTrait)
                && null != functions
                && null != functions.getComponents()
                && !functions.getComponents().isEmpty()) {
            result.addError("This store does not currently support " + storeTrait.name());
        }
    }

    private ValidationResult validateFunctionArgumentTypes(
            final ElementAggregator aggregator,
            final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final ValidationResult result = new ValidationResult();
        if (null != aggregator && null != aggregator.getComponents()) {
            for (final TupleAdaptedBinaryOperator<String, ?> adaptedFunction : aggregator.getComponents()) {
                if (null == adaptedFunction.getBinaryOperator()) {
                    result.addError(aggregator.getClass().getSimpleName() + " contains a null function.");
                } else {
                    final Signature inputSig = Signature.getInputSignature(adaptedFunction.getBinaryOperator());
                    result.add(inputSig.assignable(getTypeClasses(adaptedFunction.getSelection(), viewElDef, schemaElDef)));
                }
            }
        }

        return result;
    }

    private ValidationResult validateFunctionArgumentTypes(
            final ElementFilter filter,
            final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final ValidationResult result = new ValidationResult();
        if (null != filter && null != filter.getComponents()) {
            for (final TupleAdaptedPredicate<String, ?> adaptedPredicate : filter.getComponents()) {
                if (null == adaptedPredicate.getPredicate()) {
                    result.addError(filter.getClass().getSimpleName() + " contains a null function.");
                } else {
                    final Signature inputSig = Signature.getInputSignature(adaptedPredicate.getPredicate());
                    result.add(inputSig.assignable(getTypeClasses(adaptedPredicate.getSelection(), viewElDef, schemaElDef)));
                }
            }
        }

        return result;
    }

    private ValidationResult validateFunctionArgumentTypes(
            final ElementTransformer transformer,
            final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final ValidationResult result = new ValidationResult();
        if (null != transformer && null != transformer.getComponents()) {
            for (final TupleAdaptedFunction<String, ?, ?> adaptedFunction : transformer.getComponents()) {
                if (null == adaptedFunction.getFunction()) {
                    result.addError(transformer.getClass().getSimpleName() + " contains a null function.");
                } else {
                    final Signature inputSig = Signature.getInputSignature(adaptedFunction.getFunction());
                    result.add(inputSig.assignable(getTypeClasses(adaptedFunction.getSelection(), viewElDef, schemaElDef)));

                    final Signature outputSig = Signature.getOutputSignature(adaptedFunction.getFunction());
                    result.add(outputSig.assignable(getTypeClasses(adaptedFunction.getProjection(), viewElDef, schemaElDef)));
                }
            }
        }

        return result;
    }

    private Class[] getTypeClasses(final String[] keys, final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final Class[] selectionClasses = new Class[keys.length];
        int i = 0;
        for (final String key : keys) {
            selectionClasses[i] = getTypeClass(key, viewElDef, schemaElDef);
            i++;
        }
        return selectionClasses;
    }

    private Class<?> getTypeClass(final String key, final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final IdentifierType idType = IdentifierType.fromName(key);
        final Class<?> clazz;
        if (null != idType) {
            clazz = schemaElDef.getIdentifierClass(idType);
        } else {
            final Class<?> schemaClazz = schemaElDef.getPropertyClass(key);
            if (null != schemaClazz) {
                clazz = schemaClazz;
            } else {
                clazz = viewElDef.getTransientPropertyClass(key);
            }
        }
        if (null == clazz) {
            if (null != idType) {
                final String typeName = schemaElDef.getIdentifierTypeName(idType);
                if (null != typeName) {
                    LOGGER.debug("No class type found for type definition {} used by identifier {}. Please ensure it is defined in the schema.", typeName, idType);
                } else {
                    LOGGER.debug("No type definition defined for identifier {}. Please ensure it is defined in the schema.", idType);
                }
            } else {
                final String typeName = schemaElDef.getPropertyTypeName(key);
                if (null != typeName) {
                    LOGGER.debug("No class type found for type definition {} used by property {}. Please ensure it is defined in the schema.", typeName, key);
                } else {
                    LOGGER.debug("No class type found for transient property {}. Please ensure it is defined in the view.", key);
                }
            }
        }
        return clazz;
    }
}
