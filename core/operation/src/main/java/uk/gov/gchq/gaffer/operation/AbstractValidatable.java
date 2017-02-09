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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractValidatable<OUTPUT> extends AbstractOperation<CloseableIterable<Element>, OUTPUT> implements Validatable<OUTPUT> {
    private boolean validate;
    private boolean skipInvalidElements;

    protected AbstractValidatable() {
        this(null, null);
    }

    protected AbstractValidatable(final Iterable<Element> elements) {
        this(null, elements);
    }

    protected AbstractValidatable(final CloseableIterable<Element> elements) {
        this(null, elements);
    }

    protected AbstractValidatable(final View view) {
        this(view, null);
    }

    protected AbstractValidatable(final View view, final Iterable<Element> elements) {
        this(view, elements, true, false);
    }

    protected AbstractValidatable(final View view, final CloseableIterable<Element> elements) {
        this(view, elements, true, false);
    }

    protected AbstractValidatable(final boolean validate, final boolean skipInvalidElements) {
        this(null, null, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final Iterable<Element> elements, final boolean validate, final boolean skipInvalidElements) {
        this(null, elements, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final CloseableIterable<Element> elements, final boolean validate, final boolean skipInvalidElements) {
        this(null, elements, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final View view, final boolean validate, final boolean skipInvalidElements) {
        this(view, null, validate, skipInvalidElements);
    }

    protected AbstractValidatable(final View view, final Iterable<Element> elements, final boolean validate, final boolean skipInvalidElements) {
        this(view, new WrappedCloseableIterable<>(elements), validate, skipInvalidElements);
    }

    protected AbstractValidatable(final View view, final CloseableIterable<Element> elements, final boolean validate, final boolean skipInvalidElements) {
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
    public CloseableIterable<Element> getElements() {
        return getInput();
    }

    @Override
    public void setElements(final CloseableIterable<Element> elements) {
        setInput(elements);
    }

    public void setElements(final Iterable<Element> elements) {
        setInput(new WrappedCloseableIterable(elements));
    }

    @JsonIgnore
    @Override
    public CloseableIterable<Element> getInput() {
        return super.getInput();
    }

    @JsonProperty
    @Override
    public void setInput(final CloseableIterable<Element> input) {
        super.setInput(input);
    }

    @JsonProperty(value = "elements")
    List<Element> getElementList() {
        final Iterable<Element> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    @JsonProperty(value = "elements")
    void setElementList(final List<Element> elements) {
        setInput(new WrappedCloseableIterable<Element>(elements));
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

    public abstract static class BaseBuilder<
            OP_TYPE extends AbstractValidatable<OUTPUT>,
            OUTPUT,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, OUTPUT, ?>>
            extends AbstractOperation.BaseBuilder<OP_TYPE, CloseableIterable<Element>, OUTPUT, CHILD_CLASS> {
        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        /**
         * @param elements the {@link uk.gov.gchq.gaffer.data.element.Element}s to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setElements(CloseableIterable)
         */
        public CHILD_CLASS elements(final Element... elements) {
            if (null != elements) {
                elements(Arrays.asList(elements));
            }

            return self();
        }

        /**
         * @param elements the {@link uk.gov.gchq.gaffer.data.element.Element}s to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setElements(CloseableIterable)
         */
        public CHILD_CLASS elements(final CloseableIterable<Element> elements) {
            op.setElements(elements);
            return self();
        }

        /**
         * @param elements the {@link uk.gov.gchq.gaffer.data.element.Element}s to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setElements(CloseableIterable)
         */
        public CHILD_CLASS elements(final Iterable<Element> elements) {
            op.setElements(elements);
            return self();
        }

        /**
         * @param skipInvalidElements the skipInvalidElements flag to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setSkipInvalidElements(boolean)
         */
        public CHILD_CLASS skipInvalidElements(final boolean skipInvalidElements) {
            op.setSkipInvalidElements(skipInvalidElements);
            return self();
        }

        /**
         * @param validate the validate flag to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Validatable#setValidate(boolean)
         */
        public CHILD_CLASS validate(final boolean validate) {
            op.setValidate(validate);
            return self();
        }
    }

    public static final class Builder<OP_TYPE extends AbstractValidatable<OUTPUT>, OUTPUT>
            extends BaseBuilder<OP_TYPE, OUTPUT, Builder<OP_TYPE, OUTPUT>> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

        @Override
        protected Builder<OP_TYPE, OUTPUT> self() {
            return this;
        }
    }
}
