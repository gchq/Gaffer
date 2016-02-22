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

package gaffer.data.elementdefinition;

import gaffer.data.Validator;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.function.ConsumerFunction;
import gaffer.function.ConsumerProducerFunction;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.ConsumerProducerFunctionContext;
import gaffer.function.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypedElementDefinitionValidator<ELEMENT_DEF extends TypedElementDefinition> implements Validator<ELEMENT_DEF> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TypedElementDefinitionValidator.class);

    /**
     * Validates the {@link TypedElementDefinition} and checks each identifier and property has a type associated with it.
     *
     * @param elementDef the {@link gaffer.data.elementdefinition.TypedElementDefinition} to validate
     * @return true if the element definition is valid, otherwise false and an error is logged
     */
    public boolean validate(final ELEMENT_DEF elementDef) {
        return validateComponentTypes(elementDef);
    }

    protected boolean validateComponentTypes(final ELEMENT_DEF elementDef) {
        for (IdentifierType idType : elementDef.getIdentifiers()) {
            try {
                if (null == elementDef.getIdentifierClass(idType)) {
                    LOGGER.error("Class for " + idType + " could not be found.");
                    return false;
                }
            } catch (IllegalArgumentException e) {
                LOGGER.error("Class " + elementDef.getIdentifierClassName(idType) + " for identifier " + idType + " could not be found");
                return false;
            }
        }

        for (String propertyName : elementDef.getProperties()) {
            try {
                if (null == elementDef.getPropertyClass(propertyName)) {
                    LOGGER.error("Class for " + propertyName + " could not be found.");
                    return false;
                }
            } catch (IllegalArgumentException e) {
                LOGGER.error("Class " + elementDef.getPropertyClassName(propertyName) + " for property " + propertyName + " could not be found");
                return false;
            }
        }

        return true;
    }

    /**
     * Checks all function inputs are compatible with the property and identifier types specified.
     *
     * @param processor  the processor to validate against the element definition types
     * @param elementDef the element definition
     * @return boolean - true if function argument types are valid. Otherwise false and the reason is logged.
     */
    protected boolean validateFunctionArgumentTypes(
            final Processor<ElementComponentKey, ? extends ConsumerFunctionContext<ElementComponentKey, ? extends ConsumerFunction>> processor,
            final ELEMENT_DEF elementDef) {
        if (null != processor && null != processor.getFunctions()) {
            for (ConsumerFunctionContext<ElementComponentKey, ? extends ConsumerFunction> context : processor.getFunctions()) {
                if (null == context.getFunction()) {
                    LOGGER.error(processor.getClass().getSimpleName() + " contains a function context with a null function.");
                    return false;
                }

                if (!validateFunctionSelectionTypes(elementDef, context)) {
                    return false;
                }

                if (context instanceof ConsumerProducerFunctionContext
                        && !validateFunctionProjectionTypes(elementDef, (ConsumerProducerFunctionContext<ElementComponentKey, ? extends ConsumerFunction>) context)) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean validateFunctionSelectionTypes(final ELEMENT_DEF elementDef,
                                                   final ConsumerFunctionContext<ElementComponentKey, ? extends ConsumerFunction> context) {
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
        for (ElementComponentKey key : context.getSelection()) {
            final Class<?> clazz = elementDef.getClass(key);
            if (null == clazz || !inputTypes[i].isAssignableFrom(clazz)) {
                LOGGER.error("Function " + function.getClass().getName()
                        + " is not compatible with selection types. Function input type "
                        + inputTypes[i].getName() + " is not assignable from selection type "
                        + (null != clazz ? clazz.getName() : "with a null class."));
                return false;
            }
            i++;
        }
        return true;
    }

    private boolean validateFunctionProjectionTypes(final ELEMENT_DEF elementDef,
                                                    final ConsumerProducerFunctionContext<ElementComponentKey, ? extends ConsumerFunction> consumerProducerContext) {
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
        for (ElementComponentKey key : consumerProducerContext.getProjection()) {
            final Class<?> clazz = elementDef.getClass(key);
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
