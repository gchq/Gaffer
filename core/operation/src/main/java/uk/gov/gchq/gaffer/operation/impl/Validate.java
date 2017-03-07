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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableOperation;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.List;

/**
 * A <code>Validate</code> operation takes in {@link uk.gov.gchq.gaffer.data.element.Element}s validates them using the
 * store schema and returns the valid {@link uk.gov.gchq.gaffer.data.element.Element}s.
 * If skipInvalidElements is set to false, the handler should stop the operation if invalid elements are found.
 * The Graph will automatically add this operation prior to all {@link uk.gov.gchq.gaffer.operation.Validatable} operations when
 * executing.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.Validate.Builder
 */
public class Validate extends AbstractGetIterableOperation<Element, Element> {
    private boolean skipInvalidElements;

    /**
     * Constructs an instance with the skip invalid elements flag set to true.
     */
    public Validate() {
        this(true);
    }

    /**
     * Constructs an instance with the skip invalid elements flag set to the provided value.
     *
     * @param skipInvalidElements if true invalid elements will be skipped, otherwise an exception will be thrown.
     */
    public Validate(final boolean skipInvalidElements) {
        super();
        this.skipInvalidElements = skipInvalidElements;
    }

    /**
     * @return the input {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be validated.
     */
    public CloseableIterable<Element> getElements() {
        return getInput();
    }

    /**
     * @param elements the input {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be validated.
     */
    public void setElements(final Iterable<Element> elements) {
        setElements(new WrappedCloseableIterable<>(elements));
    }

    /**
     * @param elements the input {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be validated.
     */
    public void setElements(final CloseableIterable<Element> elements) {
        setInput(elements);
    }

    /**
     * @return true if invalid elements should be skipped, otherwise an exception should be thrown.
     */
    public boolean isSkipInvalidElements() {
        return skipInvalidElements;
    }

    /**
     * @param skipInvalidElements if true invalid elements will be skipped, otherwise an exception will be thrown.
     */
    public void setSkipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
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

    /**
     * @return the input {@link java.util.List} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    List<Element> getElementList() {
        final CloseableIterable<Element> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param elements the input {@link java.util.List} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    void setElementList(final List<Element> elements) {
        setInput(new WrappedCloseableIterable<>(elements));
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractOperation.BaseBuilder<Validate, CloseableIterable<Element>, CloseableIterable<Element>, CHILD_CLASS> {

        public BaseBuilder() {
            super(new Validate());
        }

        /**
         * @param elements the input {@link java.lang.Iterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be set on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.impl.Validate#setElements(Iterable)
         */
        public CHILD_CLASS elements(final Iterable<Element> elements) {
            op.setElements(elements);
            return self();
        }

        /**
         * @param elements the input {@link CloseableIterable} of {@link uk.gov.gchq.gaffer.data.element.Element}s to be set on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.impl.Validate#setElements(CloseableIterable)
         */
        public CHILD_CLASS elements(final CloseableIterable<Element> elements) {
            op.setElements(elements);
            return self();
        }

        /**
         * @param skipInvalidElements the skipInvalidElements flag to be set on the operation.
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.impl.Validate#setSkipInvalidElements(boolean)
         */
        public CHILD_CLASS skipInvalidElements(final boolean skipInvalidElements) {
            op.setSkipInvalidElements(skipInvalidElements);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
