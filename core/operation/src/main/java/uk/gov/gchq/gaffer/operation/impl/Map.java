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

import java.util.function.Function;

/**
 * A {@code Map} is an {@link Operation} which applies a {@link Function} to map an
 * input {@link Iterable} to an output object
 *
 * @param <I_ITEM> The object type of the input iterable
 * @param <O_ITEM> The object type of the output object
 */
public class Map<I_ITEM, O_ITEM> implements
        InputOutput<Iterable<I_ITEM>, O_ITEM> {

    private Iterable<I_ITEM> input;
    private java.util.Map<String, String> options;
    private Function<Iterable<I_ITEM>, O_ITEM> function;

    @Override
    public Map<I_ITEM, O_ITEM> shallowClone() throws CloneFailedException {
        return new Map.Builder<I_ITEM, O_ITEM>()
                .input(input)
                .options(options)
                .function(function)
                .build();
    }

    @Override
    public java.util.Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final java.util.Map<String, String> options) {
        this.options = options;
    }

    public Function<Iterable<I_ITEM>, O_ITEM> getFunction() {
        return function;
    }

    public void setFunction(final Function<Iterable<I_ITEM>, O_ITEM> function) {
        this.function = function;
    }

    @Override
    public Iterable<I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<I_ITEM> input) {
        this.input = input;
    }

    @Override
    public TypeReference<O_ITEM> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.IterableObj();
    }

    public static final class Builder<I_ITEM, O_ITEM> extends
            Operation.BaseBuilder<Map<I_ITEM, O_ITEM>, Builder<I_ITEM, O_ITEM>> implements
            InputOutput.Builder<Map<I_ITEM, O_ITEM>, Iterable<I_ITEM>, O_ITEM, Builder<I_ITEM, O_ITEM>> {
        public Builder() {
            super(new Map<>());
        }

        public Builder<I_ITEM, O_ITEM> function(final Function<Iterable<I_ITEM>, O_ITEM> func) {
            _getOp().setFunction(func);
            return _self();
        }
    }
}
