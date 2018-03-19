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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Iterables;
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
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.gaffer.operation.util.OperationConstants;
import uk.gov.gchq.koryphe.Since;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A {@code While} is an {@link Operation} which executes a provided delegate Operation,
 * either while some condition is true, (upto some global maximum),
 * or until some configurable cut-off is reached.
 * <p>The condition can either be in the form of anything that resolves to a boolean,
 * or a {@link Conditional} can be provided, to control the operation based upon the input object itself.
 * This allows the input to be transformed non-destructively,
 * before it is passed to the {@link Predicate} contained within the Conditional.</p>
 * <p>By default, the operation is configured to repeat only once.</p>
 */
@Since("1.5.0")
@JsonPropertyOrder(value = {"class", "input", "conditional", "operation", "options"}, alphabetic = true)
public class While implements InputOutput<Object, Object>,
        Operations<Operation> {

    private Object input;
    private Operation operation;
    private int repeats = OperationConstants.TIMES_DEFAULT;
    private Boolean condition;
    private Conditional conditional;
    private Map<String, String> options;

    @Override
    public Object getInput() {
        return input;
    }

    @Override
    public void setInput(final Object input) {
        this.input = input;
    }

    @Override
    public TypeReference<Object> getOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    @Override
    public While shallowClone() throws CloneFailedException {
        return new While.Builder()
                .input(input)
                .repeats(repeats)
                .condition(condition)
                .conditional(conditional)
                .operation(operation)
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

    public void setOperation(final Operation op) {
        this.operation = op;
    }

    public void setConditional(final Conditional conditional) {
        this.conditional = conditional;
    }

    public Operation getOperation() {
        return operation;
    }

    public Conditional getConditional() {
        return conditional;
    }

    public int getRepeats() {
        return repeats;
    }

    public void setRepeats(final int repeats) {
        this.repeats = repeats;
    }

    public Boolean isCondition() {
        return condition;
    }

    public void setCondition(final Boolean condition) {
        this.condition = condition;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(37, 83)
                .append(input)
                .append(repeats)
                .append(condition)
                .append(conditional)
                .append(operation)
                .append(options)
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final While filter = (While) obj;

        return new EqualsBuilder()
                .append(input, filter.getInput())
                .append(condition, filter.isCondition())
                .append(conditional, filter.conditional)
                .append(operation, filter.operation)
                .append(repeats, filter.repeats)
                .append(options, filter.options)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(input)
                .append(repeats)
                .append(condition)
                .append(conditional)
                .append(operation)
                .append(options)
                .toString();
    }

    @JsonIgnore
    @Override
    public Collection<Operation> getOperations() {
        final List<Operation> ops = new LinkedList<>();

        if (null == conditional) {
            ops.add(new OperationChain<>());
        } else {
            ops.add(OperationChain.wrap(conditional.getTransform()));
        }

        ops.add(OperationChain.wrap(operation));

        return ops;
    }

    @Override
    public void updateOperations(final Collection<Operation> operations) {
        if (operations.size() > 1) {
            operation = new OperationChain(Lists.newArrayList(operations));
        } else {
            operation = Iterables.get(operations, 0);
        }
    }

    public static final class Builder
            extends Operation.BaseBuilder<While, Builder>
            implements InputOutput.Builder<While, Object, Object, Builder> {

        public Builder() {
            super(new While());
        }

        public Builder operation(final Operation op) {
            _getOp().setOperation(op);
            return _self();
        }

        public Builder repeats(final int repeats) {
            _getOp().setRepeats(repeats);
            return _self();
        }

        public Builder condition(final Boolean condition) {
            _getOp().setCondition(condition);
            return _self();
        }

        public Builder conditional(final Conditional conditional) {
            _getOp().setConditional(conditional);
            return _self();
        }

        public Builder conditional(final Predicate predicate) {
            _getOp().setConditional(new Conditional(predicate));
            return _self();
        }

        public Builder conditional(final Predicate predicate, final Operation transform) {
            _getOp().setConditional(new Conditional(predicate, transform));
            return _self();
        }
    }
}
