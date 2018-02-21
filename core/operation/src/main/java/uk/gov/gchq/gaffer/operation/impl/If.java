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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A {@code If} is an {@link Operation} which will execute one of two Operations, based on the result of testing an input Object against a provided {@link Predicate}.
 * A simple boolean, or anything that resolves to a boolean, can also be used in place of the Predicate.
 *
 * @see If.Builder
 */
@JsonPropertyOrder(value = {"input", "condition", "predicate", "then", "otherwise", "options"}, alphabetic = true)
public class If<I, O> implements InputOutput<I, O>,
        Operations {

    private I input;
    private Map<String, String> options;
    private Boolean condition;
    private Operation then;
    private Operation otherwise;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    private Predicate<I> predicate;

    public If() {
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
    public If<I, O> shallowClone() throws CloneFailedException {
        return new If.Builder<I, O>()
                .input(input)
                .condition(condition)
                .predicate(predicate)
                .then(then)
                .otherwise(otherwise)
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

    @Override
    public Collection<Operation> getOperations() {
        if (null == then) {
            if (null == otherwise) {
                return Collections.emptyList();
            }
            return Collections.singletonList(OperationChain.wrap(otherwise));
        }

        if (null == otherwise) {
            return Collections.singletonList(OperationChain.wrap(then));
        }

        return Lists.newArrayList(OperationChain.wrap(then), OperationChain.wrap(otherwise));
    }

    @Override
    public void updateOperations(final Collection operations) {
        final Iterator<Operation> itr = operations.iterator();
        if (null == then) {
            if (!itr.hasNext()) {
                throw new IllegalArgumentException("Unable to update operations - there are not enough operations to set \"then\"");
            }
            then = itr.next();
        }

        if (null == otherwise) {
            if (!itr.hasNext()) {
                throw new IllegalArgumentException("Unable to update operations - there are not enough operations to set \"otherwise\"");
            }
            otherwise = itr.next();
        }

        if (itr.hasNext()) {
            throw new IllegalArgumentException("Unable to update operations - there are too many operations: " + operations.size());
        }
    }

    public Boolean getCondition() {
        return condition;
    }

    public void setCondition(final Boolean condition) {
        this.condition = condition;
    }

    public Operation getThen() {
        return then;
    }

    public void setThen(final Operation then) {
        this.then = then;
    }

    public Operation getOtherwise() {
        return otherwise;
    }

    public void setOtherwise(final Operation otherwise) {
        this.otherwise = otherwise;
    }

    public Predicate<I> getPredicate() {
        return predicate;
    }

    public void setPredicate(final Predicate<I> predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final If filter = (If) obj;

        return new EqualsBuilder()
                .append(condition, filter.condition)
                .append(predicate, filter.predicate)
                .append(then, filter.then)
                .append(otherwise, filter.otherwise)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 83)
                .append(condition)
                .append(predicate)
                .append(then)
                .append(otherwise)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(condition)
                .append(predicate)
                .append(then)
                .append(otherwise)
                .toString();
    }

    public static final class Builder<I, O>
            extends Operation.BaseBuilder<If<I, O>, Builder<I, O>>
            implements InputOutput.Builder<If<I, O>, I, O, Builder<I, O>> {
        public Builder() {
            super(new If<>());
        }

        public Builder<I, O> condition(final Boolean condition) {
            _getOp().setCondition(condition);
            return _self();
        }

        public Builder<I, O> predicate(final Predicate<I> predicate) {
            _getOp().setPredicate(predicate);
            return _self();
        }

        public Builder<I, O> then(final Operation op) {
            _getOp().setThen(op);
            return _self();
        }

        public Builder<I, O> otherwise(final Operation op) {
            _getOp().setOtherwise(op);
            return _self();
        }
    }
}
