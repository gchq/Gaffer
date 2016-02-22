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

package gaffer.data.elementdefinition.view;

import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.TypedElementDefinitionValidator;
import gaffer.function.TransformFunction;
import gaffer.function.context.ConsumerProducerFunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * An <code>ViewElementDefinitionValidator</code> extends
 * {@link gaffer.data.elementdefinition.TypedElementDefinitionValidator} and adds additional validation for a
 * {@link gaffer.data.elementdefinition.view.ViewElementDefinition}. To be able to aggregate 2 similar elements
 * together ALL properties have to be aggregated together. So this validator checks that either no properties have
 * aggregator functions or all properties have aggregator functions defined.
 */
public class ViewElementDefinitionValidator extends TypedElementDefinitionValidator<ViewElementDefinition> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewElementDefinitionValidator.class);

    /**
     * Carries out the validation as per {@link gaffer.data.elementdefinition.TypedElementDefinitionValidator} and then
     * checks all {@link gaffer.function.FilterFunction}s and {@link gaffer.function.TransformFunction}s defined are
     * compatible with the identifiers and properties - this is done by comparing the function input and output types with
     * the identifier and property types.
     *
     * @param elementDef the {@link gaffer.data.elementdefinition.TypedElementDefinition} to validate
     * @return true if the element definition is valid, otherwise false and an error is logged
     * @see gaffer.data.elementdefinition.TypedElementDefinitionValidator
     */
    @Override
    public boolean validate(final ViewElementDefinition elementDef) {
        final ElementFilter filter = elementDef.getFilter();
        final ElementTransformer transformer = elementDef.getTransformer();

        return super.validate(elementDef)
                && validateTransformer(transformer, elementDef)
                && validateFunctionArgumentTypes(filter, elementDef)
                && validateFunctionArgumentTypes(transformer, elementDef);
    }

    private boolean validateTransformer(final ElementTransformer transformer, final ViewElementDefinition elementDef) {
        if (null != transformer) {
            Set<String> selectProperties = new HashSet<>();
            Set<String> projectProperties = new HashSet<>();
            if (transformer.getFunctions() != null) {
                for (ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction> context : transformer.getFunctions()) {
                    if (context.getSelection() != null) {
                        for (ElementComponentKey key : context.getSelection()) {
                            if (!key.isId()) {
                                selectProperties.add(key.getPropertyName());
                            }
                        }
                    }

                    if (context.getProjection() != null) {
                        for (ElementComponentKey key : context.getProjection()) {
                            if (!key.isId()) {
                                projectProperties.add(key.getPropertyName());
                            }
                        }
                    }
                }
            }

            selectProperties.removeAll(elementDef.getProperties());
            if (selectProperties.size() > 0) {
                LOGGER.error("Selected properties '" + selectProperties.toString() + "' were not found in the property list for an element.");
                return false;
            }

            projectProperties.removeAll(elementDef.getProperties());
            if (projectProperties.size() > 0) {
                LOGGER.error("Projected properties '" + projectProperties.toString() + "' were not found in the property list for an element.");
                return false;
            }
        }

        return true;
    }
}
