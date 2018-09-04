/*
 * Copyright 2018 Crown Copyright
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
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.List;
import java.util.Map;

/**
 * A {@code ToSingletonList} operation takes an input of type T and converts a
 * single input to a list.
 *
 * @param <T> input type
 * @see uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList.Builder
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.7.0")
@Summary("Converts a single input of type T to a List")
public class ToSingletonList<T> implements InputOutput<T, List<? extends T>> {
    private T input;
    private Map<String, String> options;

    @Override
    public T getInput() {
        return input;
    }

    @Override
    public void setInput(final T input) {
        this.input = input;
    }

    @Override
    public TypeReference<List<? extends T>> getOutputTypeReference() {
        return new TypeReferenceImpl.List();
    }

    @Override
    public ToSingletonList<T> shallowClone() throws CloneFailedException {
        return new ToSingletonList.Builder<T>()
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
            extends BaseBuilder<ToSingletonList<T>, Builder<T>>
            implements InputOutput.Builder<ToSingletonList<T>, T, List<? extends T>, Builder<T>> {
        public Builder() {
            super(new ToSingletonList<>());
        }
    }
}
