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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.GenericInput;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.koryphe.Since;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static uk.gov.gchq.gaffer.operation.util.OperationUtil.extractNextOp;

/**
 * <p>
 * A {@code If} is an {@link Operation} which will execute one of two Operations,
 * based on the result of testing an input Object against a provided {@link java.util.function.Predicate}.
 * </p>
 * <p>
 * This {@code Predicate} can also be configured with an {@link Conditional},
 * which simply wraps an {@code Operation} and a {@code Predicate}.
 * This enables pre-predicate transformation of the input,
 * which allows properties other than the input object to be passed to the predicate,
 * whilst preserving the initial input.
 * </p>
 * <p>
 * As an example, this allows you to build an {@link Operation}
 * which extracts a property from the input, passes it to the predicate,
 * then the untouched original input is passed on to the operation determined by the predicate test.
 * </p>
 * <p>
 * A simple boolean, or anything that resolves to a boolean, can also be used to determine which Operation to execute.
 * </p>
 *
 * @see If.Builder
 */
@Since("1.4.0")
@JsonPropertyOrder(value = {"input", "condition", "conditional", "then", "otherwise", "options"}, alphabetic = true)
public class If<I, O> extends GenericInput<I> implements InputOutput<I, O>, Operations<Operation> {
    private Boolean condition;
    private Conditional conditional;
    private Operation then;
    private Operation otherwise;
    private Map<String, String> options;

    @Override
    public TypeReference<O> getOutputTypeReference() {
        return TypeReferenceImpl.createExplicitT();
    }

    @Override
    public If<I, O> shallowClone() throws CloneFailedException {
        If.Builder<I, O> builder = new If.Builder<I, O>()
                .input(getInput())
                .condition(condition)
                .conditional(conditional)
                .then(then)
                .otherwise(otherwise)
                .options(options);

        if (null != conditional) {
            builder = builder.conditional(conditional.shallowClone());
        }
        if (null != then) {
            builder = builder.then(then.shallowClone());
        }
        if (null != otherwise) {
            builder = builder.then(otherwise.shallowClone());
        }

        return builder.build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @JsonIgnore
    @Override
    public Collection<Operation> getOperations() {
        final List<Operation> ops = new LinkedList<>();

        if (null == conditional) {
            ops.add(new OperationChain());
        } else {
            ops.add(OperationChain.wrap(conditional.getTransform()));
        }

        ops.add(OperationChain.wrap(then));
        ops.add(OperationChain.wrap(otherwise));

        return ops;
    }

    @Override
    public void updateOperations(final Collection<Operation> operations) {
        if (null == operations || 3 != operations.size()) {
            throw new IllegalArgumentException("Unable to update operations - exactly 3 operations are required. Received " + (null != operations ? operations.size() : 0) + " operations");
        }

        final Iterator<Operation> itr = operations.iterator();
        final Operation transform = extractNextOp(itr);
        if (null == conditional) {
            if (null != transform) {
                conditional = new Conditional();
                conditional.setTransform(transform);
            }
        } else {
            conditional.setTransform(transform);
        }

        then = extractNextOp(itr);
        otherwise = extractNextOp(itr);
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

    public Conditional getConditional() {
        return conditional;
    }

    public void setConditional(final Conditional conditional) {
        this.conditional = conditional;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final If ifOp = (If) obj;

        return new EqualsBuilder()
                .append(getInput(), ifOp.getInput())
                .append(condition, ifOp.condition)
                .append(conditional, ifOp.conditional)
                .append(then, ifOp.then)
                .append(otherwise, ifOp.otherwise)
                .append(options, ifOp.options)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 83)
                .append(getInput())
                .append(condition)
                .append(conditional)
                .append(then)
                .append(otherwise)
                .append(options)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(getInput())
                .append(condition)
                .append(conditional)
                .append(then)
                .append(otherwise)
                .append(options)
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

        public Builder<I, O> conditional(final Conditional conditional) {
            _getOp().setConditional(conditional);
            return _self();
        }

        public Builder<I, O> conditional(final Predicate predicate) {
            _getOp().setConditional(new Conditional(predicate));
            return _self();
        }

        public Builder<I, O> conditional(final Predicate predicate, final Operation transform) {
            _getOp().setConditional(new Conditional(predicate, transform));
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
