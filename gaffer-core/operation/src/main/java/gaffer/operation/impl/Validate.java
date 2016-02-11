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

package gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractOperation;

import java.util.List;

/**
 * A <code>Validate</code> operation takes in {@link gaffer.data.element.Element}s validates them using the
 * {@link gaffer.data.elementdefinition.schema.DataSchema} and returns the valid {@link gaffer.data.element.Element}s.
 * If skipInvalidElements is set to false, the handler should stop the operation if invalid elements are found.
 * The Graph will automatically add this operation prior to all {@link gaffer.operation.Validatable} operations when
 * executing.
 *
 * @see gaffer.operation.impl.Validate.Builder
 */
public class Validate extends AbstractOperation<Iterable<Element>, Iterable<Element>> {
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
     * @return the input {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to be validated.
     */
    public Iterable<Element> getElements() {
        return getInput();
    }

    /**
     * @param elements the input {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to be validated.
     */
    public void setElements(final Iterable<Element> elements) {
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
    public Iterable<Element> getInput() {
        return super.getInput();
    }

    /**
     * @return the input {@link java.util.List} of {@link gaffer.data.element.Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    List<Element> getElementList() {
        final Iterable<Element> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    /**
     * @param elements the input {@link java.util.List} of {@link gaffer.data.element.Element}s to be validated.
     */
    @JsonProperty(value = "elements")
    void setElementList(final List<Element> elements) {
        setInput(elements);
    }

    public static class Builder extends AbstractOperation.Builder<Validate, Iterable<Element>, Iterable<Element>> {

        public Builder() {
            super(new Validate());
        }

        /**
         * @param elements the input {@link java.lang.Iterable} of {@link gaffer.data.element.Element}s to be set on the operation.
         * @return this Builder
         * @see gaffer.operation.impl.Validate#setElements(Iterable)
         */
        public Builder elements(final Iterable<Element> elements) {
            op.setElements(elements);
            return this;
        }

        /**
         * @param skipInvalidElements the skipInvalidElements flag to be set on the operation.
         * @return this Builder
         * @see gaffer.operation.impl.Validate#setSkipInvalidElements(boolean)
         */
        public Builder skipInvalidElements(final boolean skipInvalidElements) {
            op.setSkipInvalidElements(skipInvalidElements);
            return this;
        }

        @Override
        public Builder view(final View view) {
            super.view(view);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
