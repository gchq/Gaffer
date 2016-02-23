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

package gaffer.data;

import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.DataElementDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An <code>ElementValidator</code> is a {@link gaffer.data.Validator} for {@link gaffer.data.element.Element}s
 * It is capable of validating an {@link gaffer.data.element.Element} based on {@link gaffer.function.FilterFunction}s
 * in {@link gaffer.data.elementdefinition.schema.DataSchema} or {@link gaffer.data.elementdefinition.view.View}.
 */
public class ElementValidator implements Validator<Element> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementValidator.class);
    private final DataSchema dataSchema;
    private final View view;

    /**
     * Constructs a <code>ElementValidator</code> with a {@link gaffer.data.elementdefinition.schema.DataSchema} to use to
     * validate {@link gaffer.data.element.Element}s.
     *
     * @param dataSchema the {@link gaffer.data.elementdefinition.schema.DataSchema} to use to
     *                   validate {@link gaffer.data.element.Element}s.
     */
    public ElementValidator(final DataSchema dataSchema) {
        this.dataSchema = dataSchema;
        this.view = null;
    }

    /**
     * Constructs a <code>ElementValidator</code> with a {@link gaffer.data.elementdefinition.view.View} to use to
     * validate {@link gaffer.data.element.Element}s.
     *
     * @param view the {@link gaffer.data.elementdefinition.view.View} to use to
     *             validate {@link gaffer.data.element.Element}s.
     */
    public ElementValidator(final View view) {
        this.view = view;
        this.dataSchema = null;
    }

    /**
     * @param element the {@link gaffer.data.element.Element} to validate
     * @return true if the provided {@link gaffer.data.element.Element} is valid,
     * otherwise false and the reason will be logged.
     */
    @Override
    public boolean validate(final Element element) {
        if (null == element) {
            return false;
        }

        if (null != dataSchema) {
            return validateWithDataSchema(element);
        }

        return validateWithView(element);
    }

    private boolean validateWithDataSchema(final Element element) {
        final DataElementDefinition elementDef = dataSchema.getElement(element.getGroup());
        if (null == elementDef) {
            LOGGER.warn("No element definition found for : " + element.getGroup());
            return false;
        }

        return elementDef.getValidator().filter(element);
    }

    private boolean validateWithView(final Element element) {
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        if (null == elementDef) {
            LOGGER.warn("No element definition found for : " + element.getGroup());
            return false;
        }

        final ElementFilter validator = elementDef.getFilter();
        return null == validator || validator.filter(element);
    }
}
