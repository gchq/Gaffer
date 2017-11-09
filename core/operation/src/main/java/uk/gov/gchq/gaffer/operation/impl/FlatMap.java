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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Map;
import java.util.function.Function;

/**
 * A {@code FlatMap} is an {@link Operation} which takes an {@link Iterable} of {@link Iterable}s,
 * applies a function to map each of the nested {@link Iterable}s to an object, and returns
 * an {@link Iterable} containing the resulting objects.
 *
 * @param <I_ITEM> The object type of the nested iterables
 * @param <O_ITEM> the object type of the output iterable
 */
public class FlatMap<I_ITEM, O_ITEM> implements
        InputOutput<Iterable<Iterable<I_ITEM>>, Iterable<O_ITEM>> {
    private Iterable<Iterable<I_ITEM>> input;
    private Map<String, String> options;
    private Function<Iterable<I_ITEM>, O_ITEM> function;

    public FlatMap() {
        // Empty
    }

    @Override
    public Iterable<Iterable<I_ITEM>> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<Iterable<I_ITEM>> input) {
        this.input = input;
    }

    @Override
    public FlatMap<I_ITEM, O_ITEM> shallowClone() throws CloneFailedException {
        return new FlatMap.Builder<I_ITEM, O_ITEM>()
                .input(input)
                .options(options)
                .function(function)
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

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<Iterable<I_ITEM>, O_ITEM> getFunction() {
        return function;
    }

    public void setFunction(final Function<Iterable<I_ITEM>, O_ITEM> function) {
        this.function = function;
    }

    @Override
    public TypeReference<Iterable<O_ITEM>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableExplicitT();
    }

    public static final class Builder<I_ITEM, O_ITEM> extends
            Operation.BaseBuilder<FlatMap<I_ITEM, O_ITEM>, Builder<I_ITEM, O_ITEM>> implements
            InputOutput.Builder<FlatMap<I_ITEM, O_ITEM>, Iterable<Iterable<I_ITEM>>, Iterable<O_ITEM>, Builder<I_ITEM, O_ITEM>> {
        public Builder() {
            super(new FlatMap<>());
        }

        public Builder<I_ITEM, O_ITEM> function(final Function<Iterable<I_ITEM>, O_ITEM> func) {
            _getOp().setFunction(func);
            return _self();
        }
    }
}
