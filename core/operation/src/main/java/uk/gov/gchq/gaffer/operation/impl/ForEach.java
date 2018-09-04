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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code ForEach} operation runs the supplied operation on an Iterable of inputs.
 *
 * @param <T> input type in iterable
 * @param <U> output type in iterable
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.7.0")
@Summary("Runs supplied operation on Iterable of inputs")
public class ForEach<T, U> implements InputOutput<Iterable<? extends T>, Iterable<? extends U>> {
    private Iterable<? extends T> input;
    private Class<? extends Operation> operation;
    private Map<String, String> options;

    @Override
    public Iterable<? extends T> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends T> input) {
        this.input = input;
    }

    public Class<? extends Operation> getOperation() {
        return operation;
    }

    public void setOperation(final Class<? extends Operation> operation) {
        this.operation = operation;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public ForEach<T,U> shallowClone() throws CloneFailedException {
        return new ForEach.Builder<T, U>()
                .input(input)
                .operation(operation)
                .options(options)
                .build();
    }

    @Override
    public TypeReference<Iterable<? extends U>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableT();
    }

    public static final class Builder<T, U>
            extends BaseBuilder<ForEach<T, U>, Builder<T, U>>
            implements InputOutput.Builder<ForEach<T, U>, Iterable<? extends T>, Iterable<? extends U>, Builder<T, U>> {

        public Builder() {
            super(new ForEach<>());
        }

        public Builder<T, U> operation(final Class<? extends Operation> operation) {
            _getOp().setOperation(operation);
            return _self();
        }
    }
}
