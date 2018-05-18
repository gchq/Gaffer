/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.add;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.io.AbstractIOOperation;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

/**
 * An {@code AddElements} operation is a {@link uk.gov.gchq.gaffer.operation.Validatable} operation for adding elements.
 * This is a core operation that all stores should be able to handle.
 * This operation requires an {@link Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be added. Handlers should
 * throw an {@link uk.gov.gchq.gaffer.operation.OperationException} if unsuccessful.
 * For normal operation handlers the operation {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} will be ignored.
 */
@JsonPropertyOrder(value = {"class", "elements"}, alphabetic = true)
@Since("1.0.0")
@Summary("Adds elements")
public class AddElements extends AbstractIOOperation<AddElements, Element>
        implements
        Validatable,
        MultiInput<Element> {
    private boolean validate = true;
    private boolean skipInvalidElements;

    @Override
    public boolean isValidate() {
        return validate;
    }

    @Override
    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    @Override
    public boolean isSkipInvalidElements() {
        return skipInvalidElements;
    }

    @Override
    public void setSkipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @Override
    public Object[] createInputArray() {
        return MultiInput.super.createInputArray();
    }


    @Override
    public AddElements shallowClone() {
        return new AddElements()
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .input(input)
                .options(options);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final AddElements addElements = (AddElements) obj;

        return new EqualsBuilder()
                .append(options, addElements.options)
                .append(validate, addElements.validate)
                .append(skipInvalidElements, addElements.skipInvalidElements)
                .append(input, addElements.input)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(67, 23)
                .append(options)
                .append(validate)
                .append(skipInvalidElements)
                .append(input)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("options", options)
                .append("validate", validate)
                .append("skipInvalidElements", skipInvalidElements)
                .append("elements", input)
                .toString();
    }

    public AddElements validate(final boolean validate) {
        this.validate = validate;
        return this;
    }

    public AddElements skipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
        return this;
    }

}
