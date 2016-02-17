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

package gaffer.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;

import java.util.List;

public abstract class AbstractValidatable<OUTPUT> extends AbstractOperation<Iterable<Element>, OUTPUT> implements Validatable<OUTPUT> {
    private boolean validate;
    private boolean skipInvalidElements;

    protected AbstractValidatable() {
        this(null, null);
    }

    protected AbstractValidatable(final Iterable<Element> elements) {
        this(null, elements);
    }

    protected AbstractValidatable(final View view) {
        this(view, null);
    }

    protected AbstractValidatable(final View view, final Iterable<Element> elements) {
        this(view, elements, true, false);
    }

    protected AbstractValidatable(final boolean validate, final boolean skipInvalidElements) {
        this(null, null, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final Iterable<Element> elements, final boolean validate, final boolean skipInvalidElements) {
        this(null, elements, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final View view, final boolean validate, final boolean skipInvalidElements) {
        this(view, null, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final View view, final Iterable<Element> elements, final boolean validate, final boolean skipInvalidElements) {
        super(view, elements);
        this.validate = validate;
        this.skipInvalidElements = skipInvalidElements;
    }

    protected AbstractValidatable(final AbstractValidatable<?> operation) {
        super(operation);
        setValidate(operation.isValidate());
        setSkipInvalidElements(operation.isSkipInvalidElements());
    }

    @Override
    public Iterable<Element> getElements() {
        return getInput();
    }

    @Override
    public void setElements(final Iterable<Element> elements) {
        setInput(elements);
    }

    @JsonIgnore
    @Override
    public Iterable<Element> getInput() {
        return super.getInput();
    }

    @JsonProperty(value = "elements")
    List<Element> getElementList() {
        final Iterable<Element> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    @JsonProperty(value = "elements")
    void setElementList(final List<Element> elements) {
        setInput(elements);
    }

    @Override
    public boolean isSkipInvalidElements() {
        return skipInvalidElements;
    }

    @Override
    public boolean isValidate() {
        return validate;
    }

    @Override
    public void setSkipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
    }

    @Override
    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    public static class Builder<OP_TYPE extends AbstractValidatable<OUTPUT>, OUTPUT>
            extends AbstractOperation.Builder<OP_TYPE, Iterable<Element>, OUTPUT> {
        protected Builder(final OP_TYPE op) {
            super(op);
        }

        /**
         * @param elements the {@link gaffer.data.element.Element}s to set on the operation
         * @return this Builder
         * @see gaffer.operation.Validatable#setElements(Iterable)
         */
        public Builder<OP_TYPE, OUTPUT> elements(final Iterable<Element> elements) {
            op.setElements(elements);
            return this;
        }

        /**
         * @param skipInvalidElements the skipInvalidElements flag to set on the operation
         * @return this Builder
         * @see gaffer.operation.Validatable#setSkipInvalidElements(boolean)
         */
        public Builder<OP_TYPE, OUTPUT> skipInvalidElements(final boolean skipInvalidElements) {
            op.setSkipInvalidElements(skipInvalidElements);
            return this;
        }

        /**
         * @param validate the validate flag to set on the operation
         * @return this Builder
         * @see gaffer.operation.Validatable#setValidate(boolean)
         */
        public Builder<OP_TYPE, OUTPUT> validate(final boolean validate) {
            op.setValidate(validate);
            return this;
        }
    }
}
