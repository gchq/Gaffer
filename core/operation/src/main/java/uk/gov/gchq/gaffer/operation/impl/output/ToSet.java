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

package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;
import java.util.Set;

/**
 * A {@code ToSet} operation takes in an {@link java.lang.Iterable} of items
 * and converts them to a {@link java.util.Set}, removing duplicates in the
 * process.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.output.ToSet.Builder
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.0.0")
public class ToSet<T> implements
        InputOutput<Iterable<? extends T>, Set<? extends T>>,
        MultiInput<T> {
    private Iterable<? extends T> input;
    private Map<String, String> options;

    @Override
    public Iterable<? extends T> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends T> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Set<? extends T>> getOutputTypeReference() {
        return new TypeReferenceImpl.Set();
    }

    @Override
    public ToSet shallowClone() {
        return new ToSet.Builder<T>()
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

    public static final class Builder<T>
            extends BaseBuilder<ToSet<T>, Builder<T>>
            implements InputOutput.Builder<ToSet<T>, Iterable<? extends T>, Set<? extends T>, Builder<T>>,
            MultiInput.Builder<ToSet<T>, T, Builder<T>> {
        public Builder() {
            super(new ToSet<>());
        }
    }
}
