/*
 * Copyright 2017-2018 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A {@code Map} is a Gaffer {@link Operation} which maps an input I to an output O
 * by applying a supplied {@link Function} or {@link List} of {@link Function}s.
 *
 * @param <I> the type of the input object
 * @param <O> the type of the output object
 */
@JsonPropertyOrder(value = {"class", "input", "functions"}, alphabetic = true)
@Since("1.2.0")
@Summary("Maps an input to an output using provided functions")
public class Map<I, O> implements InputOutput<I, O> {
    private I input;
    private java.util.Map<String, String> options;
    @Required
    private List<Function> functions;

    public Map() {
        this(new ArrayList<>());
    }

    public Map(final Function function) {
        this(Collections.singletonList(function));
    }

    public Map(final List<Function> functions) {
        this.functions = functions;
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
        final Map<I, O> clone = new Map<>();
        for (final Function func : functions) {
            clone.getFunctions().add(func);
        }
        clone.setInput(input);
        clone.setOptions(options);
        return clone;
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
    public List<Function> getFunctions() {
        return functions;
    }

    public void setFunctions(final List<Function> funcs) {
        this.functions = funcs;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public void setFunction(final Function function) {
        this.functions = new ArrayList<>();
        functions.add(function);
    }

    public static final class Builder<I> extends
            Operation.BaseBuilder<Map<I, Object>, Builder<I>> implements
            InputOutput.Builder<Map<I, Object>, I, Object, Builder<I>> {
        public Builder() {
            super(new Map<>());
        }

        public <O> OutputBuilder<I, O> first(final Function<? extends I, O> function) {
            return new OutputBuilder<>(function, _getOp());
        }
    }

    public static final class OutputBuilder<I, O> extends
            Operation.BaseBuilder<Map<I, O>, OutputBuilder<I, O>> implements
            InputOutput.Builder<Map<I, O>, I, O, OutputBuilder<I, O>> {
        private OutputBuilder(final Function<? extends I, O> function, final Map<I, ?> operation) {
            super((Map) operation);
            if (null == operation.functions) {
                operation.functions = new ArrayList<>();
            }
            operation.functions.add(function);
        }

        public <NEXT> OutputBuilder<I, NEXT> then(final Function<? extends O, NEXT> function) {

            return new OutputBuilder(function, _getOp());
        }
    }
}
