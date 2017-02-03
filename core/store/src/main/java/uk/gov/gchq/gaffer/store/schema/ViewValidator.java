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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.ConsumerFunction;
import uk.gov.gchq.gaffer.function.ConsumerProducerFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext;
import uk.gov.gchq.gaffer.function.processor.Processor;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;

/**
 * An <code>ViewValidator</code> validates a view against a {@link Schema}
 * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition}.
 * Checks all function input and output types are compatible with the
 * properties and identifiers in the Schema and the transient properties in the
 * View.
 */
public class ViewValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewValidator.class);

    /**
     * Checks all {@link uk.gov.gchq.gaffer.function.FilterFunction}s and {@link uk.gov.gchq.gaffer.function.TransformFunction}s defined are
     * compatible with the identifiers and properties in the {@link Schema}
     * and transient properties in the {@link View}.
     *
     * @param view           the {@link View} to validate
     * @param schema         the {@link Schema} to validate the view against
     * @param isStoreOrdered true if the store is ordered
     * @return true if the element definition is valid, otherwise false and an error is logged
     */
    public boolean validate(final View view, final Schema schema, final boolean isStoreOrdered) {
        boolean isValid = true;

        if (null != view) {
            if (null != view.getEntities()) {
                for (final Entry<String, ViewElementDefinition> entry : view.getEntities().entrySet()) {
                    final String group = entry.getKey();
                    final SchemaEntityDefinition schemaElDef = schema.getEntity(group);
                    final ViewElementDefinition viewElDef = entry.getValue();

                    if (null == schemaElDef) {
                        LOGGER.error("Entity group " + group + " does not exist in the schema");
                        isValid = false;
                    } else {
                        for (final String transProp : viewElDef.getTransientProperties()) {
                            if (schemaElDef.containsProperty(transProp)) {
                                LOGGER.error("Transient property " + transProp + " for entity group " + group + " is not transient as it has been found in the schema");
                                isValid = false;
                            }
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getPreAggregationFilter(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getPostAggregationFilter(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getTransformer(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getPostTransformFilter(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateGroupBy(isStoreOrdered, group, viewElDef, schemaElDef)) {
                            isValid = false;
                        }
                    }
                }
            }

            if (null != view.getEdges()) {
                for (final Entry<String, ViewElementDefinition> entry : view.getEdges().entrySet()) {
                    final String group = entry.getKey();
                    final SchemaEdgeDefinition schemaElDef = schema.getEdge(group);
                    final ViewElementDefinition viewElDef = entry.getValue();

                    if (null == schemaElDef) {
                        LOGGER.error("Edge group " + group + " does not exist in the schema");
                        isValid = false;
                    } else {
                        for (final String transProp : viewElDef.getTransientProperties()) {
                            if (schemaElDef.containsProperty(transProp)) {
                                LOGGER.error("Transient property " + transProp + " for edge group " + group + " is not transient as it has been found in the schema");
                                isValid = false;
                            }
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getPreAggregationFilter(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getPostAggregationFilter(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getTransformer(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateFunctionArgumentTypes(viewElDef.getPostTransformFilter(), viewElDef, schemaElDef)) {
                            isValid = false;
                        }

                        if (!validateGroupBy(isStoreOrdered, group, viewElDef, schemaElDef)) {
                            isValid = false;
                        }
                    }
                }
            }
        }

        return isValid;
    }

    protected boolean validateGroupBy(final boolean isStoreOrdered, final String group, final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        final Set<String> viewGroupBy = viewElDef.getGroupBy();

        boolean isValid = true;
        if (null != viewGroupBy && !viewGroupBy.isEmpty()) {
            final Set<String> schemaGroupBy = schemaElDef.getGroupBy();
            if (null != schemaGroupBy && schemaGroupBy.containsAll(viewGroupBy)) {
                if (isStoreOrdered) {
                    final LinkedHashSet<String> schemaGroupBySubset = Sets.newLinkedHashSet(Iterables.limit(schemaGroupBy, viewGroupBy.size()));
                    if (!viewGroupBy.equals(schemaGroupBySubset)) {
                        LOGGER.error("Group by properties for group " + group + " are not in the same order as the group by properties in the schema. View groupBy:" + viewGroupBy + ". Schema groupBy:" + schemaGroupBy);
                        isValid = false;
                    }
                }
            } else {
                LOGGER.error("Group by properties for group " + group + " in the view are not all included in the group by field in the schema. View groupBy:" + viewGroupBy + ". Schema groupBy:" + schemaGroupBy);
                isValid = false;
            }
        }

        return isValid;
    }


    /**
     * Checks all function inputs and outputs are compatible with the property, identifier types
     * and transient properties specified.
     *
     * @param processor   the processor to validate against the element definition types
     * @param viewElDef   the view element definition
     * @param schemaElDef the schema element definition
     * @return boolean - true if function argument types are valid. Otherwise false and the reason is logged.
     */

    private boolean validateFunctionArgumentTypes(
            final Processor<String, ? extends ConsumerFunctionContext<String, ? extends ConsumerFunction>> processor,
            final ViewElementDefinition viewElDef, final SchemaElementDefinition schemaElDef) {
        if (null != processor && null != processor.getFunctions()) {
            for (final ConsumerFunctionContext<String, ? extends ConsumerFunction> context : processor.getFunctions()) {
                if (null == context.getFunction()) {
                    LOGGER.error(processor.getClass().getSimpleName() + " contains a function context with a null function.");
                    return false;
                }

                if (!validateFunctionSelectionTypes(viewElDef, schemaElDef, context)) {
                    return false;
                }

                if (context instanceof ConsumerProducerFunctionContext
                        && !validateFunctionProjectionTypes(viewElDef, schemaElDef, (ConsumerProducerFunctionContext<String, ? extends ConsumerFunction>) context)) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean validateFunctionSelectionTypes(final ViewElementDefinition viewElDef,
                                                   final SchemaElementDefinition schemaElDef,
                                                   final ConsumerFunctionContext<String, ? extends ConsumerFunction> context) {
        final ConsumerFunction function = context.getFunction();
        final Class<?>[] inputTypes = function.getInputClasses();
        if (null == inputTypes || 0 == inputTypes.length) {
            LOGGER.error("Function " + function.getClass().getName()
                    + " is invalid. Input types have not been set.");
            return false;
        }

        if (inputTypes.length != context.getSelection().size()) {
            LOGGER.error("Input types for function " + function.getClass().getName()
                    + " are not equal to the selection property types.");
            return false;
        }

        int i = 0;
        for (final String key : context.getSelection()) {
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
                        LOGGER.error("No class type found for type definition " + typeName
                                + " used by identifier " + idType
                                + ". Please ensure it is defined in the schema.");
                    } else {
                        LOGGER.error("No type definition defined for identifier " + idType
                                + ". Please ensure it is defined in the schema.");
                    }
                } else {
                    final String typeName = schemaElDef.getPropertyTypeName(key);
                    if (null != typeName) {
                        LOGGER.error("No class type found for type definition " + typeName
                                + " used by property " + key
                                + ". Please ensure it is defined in the schema.");
                    } else {
                        LOGGER.error("No class type found for transient property " + key
                                + ". Please ensure it is defined in the view.");
                    }
                }

                return false;
            }

            if (!inputTypes[i].isAssignableFrom(clazz)) {
                LOGGER.error("Function " + function.getClass().getName()
                        + " is not compatible with selection types. Function input type "
                        + inputTypes[i].getName() + " is not assignable from selection type "
                        + clazz.getName() + ".");
                return false;
            }
            i++;
        }

        return true;
    }

    private boolean validateFunctionProjectionTypes(final ViewElementDefinition viewElDef,
                                                    final SchemaElementDefinition schemaElDef,
                                                    final ConsumerProducerFunctionContext<String, ? extends ConsumerFunction> consumerProducerContext) {
        final ConsumerProducerFunction function = consumerProducerContext.getFunction();
        final Class<?>[] outputTypes = function.getOutputClasses();
        if (null == outputTypes || 0 == outputTypes.length) {
            LOGGER.error("Function " + function.getClass().getName()
                    + " is invalid. Output types have not been set.");
            return false;
        }

        if (outputTypes.length != consumerProducerContext.getProjection().size()) {
            LOGGER.error("Output types for function " + function.getClass().getName()
                    + " are not equal to the projection property types.");
            return false;
        }

        int i = 0;
        for (final String key : consumerProducerContext.getProjection()) {
            final Class<?> clazz;
            final IdentifierType idType = IdentifierType.fromName(key);
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
            if (null == clazz || !outputTypes[i].isAssignableFrom(clazz)) {
                LOGGER.error("Function " + function.getClass().getName()
                        + " is not compatible with output types. Function output type "
                        + outputTypes[i].getName() + " is not assignable from projection type "
                        + (null != clazz ? clazz.getName() : "with a null class."));
                return false;
            }
            i++;
        }
        return true;
    }

}
