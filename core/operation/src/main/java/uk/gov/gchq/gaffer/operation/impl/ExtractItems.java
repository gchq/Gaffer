/*
 * Copyright 2017 Crown Copyright
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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Map;

/**
 * An {@code ExtractItems} is an {@link Operation} which takes an {@link Iterable} of {@link Iterable}s,
 * extracts a specific object based on user selection, and returns an {@link Iterable}.
 */
public class ExtractItems implements InputOutput<Iterable<Iterable<? extends Object>>, Iterable<? extends Object>> {
    private Iterable<Iterable<? extends Object>> input;
    private Map<String, String> options;
    private int selection;

    public ExtractItems() {
        // blank
    }

    public ExtractItems(final Iterable<Iterable<? extends Object>> input, final int selection) {
        this.input = input;
        this.selection = selection;
    }

    @Override
    public Iterable<Iterable<? extends Object>> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<Iterable<? extends Object>> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableObject();
    }

    @Override
    public ExtractItems shallowClone() throws CloneFailedException {
        return new ExtractItems.Builder()
                .input(input)
                .options(options)
                .selection(selection)
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

    public int getSelection() {
        return selection;
    }

    public void setSelection(final int selection) {
        this.selection = selection;
    }

    public static final class Builder
            extends Operation.BaseBuilder<ExtractItems, Builder>
            implements InputOutput.Builder<ExtractItems, Iterable<Iterable<? extends Object>>, Iterable<? extends Object>, Builder> {

        public Builder() {
            super(new ExtractItems());
        }

        public Builder selection(final int selection) {
            _getOp().selection = selection;
            return _self();
        }
    }
}
