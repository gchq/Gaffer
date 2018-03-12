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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;

/**
 * A {@code Validate} operation takes in {@link uk.gov.gchq.gaffer.data.element.Element}s validates them using the
 * store schema and returns the valid {@link uk.gov.gchq.gaffer.data.element.Element}s.
 * If skipInvalidElements is set to false, the handler should stop the operation if invalid elements are found.
 * The Graph will automatically add this operation prior to all {@link uk.gov.gchq.gaffer.operation.Validatable} operations when
 * executing.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.Validate.Builder
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.0.0")
public class Validate implements
        Validatable,
        InputOutput<Iterable<? extends Element>, Iterable<? extends Element>>,
        MultiInput<Element> {
    private boolean validate = true;
    private boolean skipInvalidElements;
    private Iterable<? extends Element> input;
    private Map<String, String> options;

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

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public Validate shallowClone() {
        return new Validate.Builder()
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .input(input)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static final class Builder
            extends Operation.BaseBuilder<Validate, Builder>
            implements Validatable.Builder<Validate, Builder>,
            InputOutput.Builder<Validate, Iterable<? extends Element>, Iterable<? extends Element>, Builder>,
            MultiInput.Builder<Validate, Element, Builder> {
        public Builder() {
            super(new Validate());
        }
    }
}
