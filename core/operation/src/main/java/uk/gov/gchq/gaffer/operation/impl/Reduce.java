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
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.function.BinaryOperator;

/**
 * A {@code Reduce} is a Gaffer {@code Operation} which reduces an {@link Iterable}
 * input of T to a single output value by applying a supplied {@link BinaryOperator}.
 *
 * @param <T> the type of the output object (and also the type of object held in
 *            the input {@link Iterable}.
 */
@JsonPropertyOrder(value = {"class", "input", "aggregateFunction", "identity"}, alphabetic = true)
@Since("1.7.0")
@Summary("Reduces an input to an output with a single value using provided function")
public class Reduce<T> implements InputOutput<Iterable<? extends T>, T>, MultiInput<T> {
    private Iterable<? extends T> input;
    private T identity;
    private java.util.Map<String, String> options;
    @Required
    private BinaryOperator<T> aggregateFunction;

    public Reduce() {
        // Empty
    }

    public Reduce(final BinaryOperator<T> aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    public Iterable<? extends T> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends T> input) {
        this.input = input;
    }

    public T getIdentity() {
        return identity;
    }

    public void setIdentity(final T identity) {
        this.identity = identity;
    }

    @Override
    public TypeReference<T> getOutputTypeReference() {
        return TypeReferenceImpl.createExplicitT();
    }

    @Override
    public Reduce<T> shallowClone() throws CloneFailedException {
        final Reduce<T> clone = new Reduce<>();
        clone.setAggregateFunction(aggregateFunction);
        clone.setIdentity(identity);
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
    public BinaryOperator<T> getAggregateFunction() {
        return aggregateFunction;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public void setAggregateFunction(final BinaryOperator<T> aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    public static final class Builder<T> extends
            BaseBuilder<Reduce<T>, Builder<T>> implements
            InputOutput.Builder<Reduce<T>, Iterable<? extends T>, T, Builder<T>> {
        public Builder() {
            super(new Reduce<>());
        }

        public Builder<T> aggregateFunction(final BinaryOperator aggregateFunction) {
            if (null != aggregateFunction) {
                _getOp().setAggregateFunction(aggregateFunction);
            }
            return _self();
        }

        public Builder<T> identity(final T identity) {
            if (null != identity) {
                _getOp().setIdentity(identity);
            }
            return _self();
        }
    }
}
