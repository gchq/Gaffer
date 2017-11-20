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

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.function.Function;

/**
 * A {@code Map} is a Gaffer {@link Operation} which maps an input I to an output O
 * by applying a supplied {@link Function}.
 * @param <I>   the type of the input object
 * @param <O>   the type of the output object
 */
public class Map<I, O> implements InputOutput<I, O> {
    private I input;
    private java.util.Map<String, String> options;
    @Required
    private Function<I, O> function;

    public Map() {
        // Empty
    }

    @Override
    public I getInput() {
        return input;
    }

    @Override
    public void setInput(final I input) {
        this.input = input;
    }

    @Override
    public TypeReference<O> getOutputTypeReference() {
        return TypeReferenceImpl.createExplicitT();
    }

    @Override
    public Map<I, O> shallowClone() throws CloneFailedException {
        return new Map.Builder<I, O>()
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

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<I, O> getFunction() {
        return function;
    }

    public void setFunction(final Function<I, O> function) {
        this.function = function;
    }

    public static final class Builder<I, O> extends
            Operation.BaseBuilder<Map<I, O>, Builder<I, O>> implements
            InputOutput.Builder<Map<I, O>, I, O, Builder<I, O>> {
        public Builder() { super(new Map<>()); }

        public Builder<I, O> function(final Function<I, O> func) {
            _getOp().setFunction(func);
            return _self();
        }
    }
}
