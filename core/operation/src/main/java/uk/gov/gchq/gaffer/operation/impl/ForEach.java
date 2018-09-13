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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Collection;
import java.util.Map;

/**
 * A {@code ForEach} operation runs the supplied operation on an Iterable of inputs.
 * <p>
 * For the given iterable of inputs, it will run the supplied operation for each input one at a time.
 * <p>
 * For example, a ForEach operation with:
 * <p>
 * input = [
 * [1,2,3],
 * [4,5],
 * [6]
 * ]
 * operation = Count
 * <p>
 * The results would be:
 * <p>
 * [
 * 3,
 * 2,
 * 1
 * ]
 *
 * @param <I> the type of items in the input iterable. This is the same type as the input to the supplied Operation.
 * @param <O> the type of items in the output iterable. This is the same type as the output from the supplied Operation.
 */
@JsonPropertyOrder(value = {"class", "input", "operation"}, alphabetic = true)
@Since("1.7.0")
@Summary("Runs supplied operation on Iterable of inputs")
public class ForEach<I, O> implements InputOutput<Iterable<? extends I>, Iterable<? extends O>>,
        MultiInput<I>,
        Operations<Operation> {
    private Iterable<? extends I> input;
    private Operation operation;
    private Map<String, String> options;

    @Override
    public Iterable<? extends I> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends I> input) {
        this.input = input;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(final Operation operation) {
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
    public ForEach<I, O> shallowClone() throws CloneFailedException {
        return new ForEach.Builder<I, O>()
                .input(input)
                .operation(operation)
                .options(options)
                .build();
    }

    @Override
    public TypeReference<Iterable<? extends O>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableT();
    }

    @Override
    public void updateOperations(final Collection<Operation> operations) {
        this.operation = new OperationChain<>(Lists.newArrayList(operations));
    }

    @JsonIgnore
    @Override
    public Collection<Operation> getOperations() {
        return OperationChain.wrap(operation).getOperations();
    }

    public static final class Builder<I, O>
            extends BaseBuilder<ForEach<I, O>, Builder<I, O>>
            implements InputOutput.Builder<ForEach<I, O>,
            Iterable<? extends I>, Iterable<? extends O>,
            Builder<I, O>>,
            MultiInput.Builder<ForEach<I, O>, I, Builder<I, O>> {
        public Builder() {
            super(new ForEach<>());
        }

        public Builder<I, O> operation(final Operation operation) {
            _getOp().setOperation(operation);
            return _self();
        }
    }
}

