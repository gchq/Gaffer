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

package gaffer.data.elementdefinition.schema;

import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.TypedElementDefinitionValidator;
import gaffer.function.AggregateFunction;
import gaffer.function.context.PassThroughFunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An <code>DataElementDefinitionValidator</code> extends
 * {@link gaffer.data.elementdefinition.TypedElementDefinitionValidator} and adds additional validation for a
 * {@link gaffer.data.elementdefinition.schema.DataElementDefinition}. To be able to aggregate 2 similar elements
 * together ALL properties have to be aggregated together. So this validator checks that either no properties have
 * aggregator functions or all properties have aggregator functions defined.
 */
public class DataElementDefinitionValidator extends TypedElementDefinitionValidator<DataElementDefinition> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataElementDefinitionValidator.class);

    /**
     * Carries out the validation as per {@link gaffer.data.elementdefinition.TypedElementDefinitionValidator} and then
     * checks all {@link gaffer.function.FilterFunction}s and {@link gaffer.function.AggregateFunction}s defined are
     * compatible with the identifiers and properties - this is done by comparing the function input and output types with
     * the identifier and property types.
     *
     * @param elementDef the {@link gaffer.data.elementdefinition.TypedElementDefinition} to validate
     * @return true if the element definition is valid, otherwise false and an error is logged
     * @see gaffer.data.elementdefinition.TypedElementDefinitionValidator
     */
    @Override
    public boolean validate(final DataElementDefinition elementDef) {
        final ElementFilter validator = elementDef.getValidator();
        final ElementAggregator aggregator = elementDef.getAggregator();
        return super.validate(elementDef)
                && validateAggregator(aggregator, elementDef)
                && validateFunctionArgumentTypes(validator, elementDef)
                && validateFunctionArgumentTypes(aggregator, elementDef);
    }

    private boolean validateAggregator(final ElementAggregator aggregator, final DataElementDefinition elementDef) {
        if (null == aggregator || null == aggregator.getFunctions()) {
            // if aggregate functions are not defined then it is valid
            return true;
        }

        // if aggregate functions are defined then check all properties are aggregated
        final Set<String> aggregatedProperties = new HashSet<>();
        if (aggregator.getFunctions() != null) {
            for (PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context : aggregator.getFunctions()) {
                List<ElementComponentKey> selection = context.getSelection();
                if (selection != null) {
                    for (ElementComponentKey key : selection) {
                        if (!key.isId()) {
                            aggregatedProperties.add(key.getPropertyName());
                        }
                    }
                }
            }
        }

        final Set<String> propertyNamesTmp = new HashSet<>(elementDef.getProperties());
        propertyNamesTmp.removeAll(aggregatedProperties);
        if (propertyNamesTmp.isEmpty()) {
            return true;
        }

        LOGGER.error("no aggregator found for properties '" + propertyNamesTmp.toString() + "' in the supplied schema. This framework requires that either all of the defined properties have a function associated with them, or none of them do.");
        return false;
    }
}
