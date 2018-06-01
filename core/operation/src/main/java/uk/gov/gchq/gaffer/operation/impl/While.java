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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.GenericInput;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static uk.gov.gchq.gaffer.operation.util.OperationUtil.extractNextOp;

/**
 * A {@code While} is an {@link Operation} which executes a provided delegate Operation,
 * either while some condition is true, (upto some global maximum),
 * or until some configurable cut-off is reached.
 * <p>The condition can either be in the form of anything that resolves to a boolean,
 * or a {@link Conditional} can be provided, to control the operation based upon the input object itself.
 * This allows the input to be transformed non-destructively,
 * before it is passed to the {@link Predicate} contained within the Conditional.</p>
 * <p>N.B. Only one of the condition and conditional can be set -
 * attempting to configure both will throw an exception.</p>
 * <p>By default, the operation is configured with a large number of repeats,
 * which will need altering using {@link While#setMaxRepeats(int)}.</p>
 */
@Since("1.5.0")
@Summary("Repeatedly executes an operation while a condition is met")
@JsonPropertyOrder(value = {"class", "input", "conditional", "operation", "maxRepeats", "options"}, alphabetic = true)
public class While<I, O> extends GenericInput<I> implements InputOutput<I, O>,
        Operations<Operation> {

    public static final int MAX_REPEATS = 1000;

    private Operation operation;
    private int maxRepeats = MAX_REPEATS;

    /**
     * A boolean as to whether or not the While logic should run.
     */
    private Boolean condition;

    /**
     * A {@link Conditional}, containing a transform {@link Operation}
     * and a {@link Predicate} against which the input will be tested,
     * determining whether or not the While logic should run.
     */
    private Conditional conditional;
    private Map<String, String> options;


    @Override
    public TypeReference<O> getOutputTypeReference() {
        return TypeReferenceImpl.createExplicitT();
    }

    @Override
    public While<I, O> shallowClone() {
        While.Builder<I, O> builder = new Builder<I, O>()
                .input(getInput())
                .maxRepeats(maxRepeats)
                .condition(condition)
                .options(options);

        if (null != conditional) {
            builder = builder.conditional(conditional.shallowClone());
        }
        if (null != operation) {
            builder = builder.operation(operation.shallowClone());
        }

        return builder.build();
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
        if (null == operations || 2 != operations.size()) {
            throw new IllegalArgumentException("Unable to update operations - exactly 2 operations are required. Received "
                    + (null != operations ? operations.size() : 0) + " operations.");
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

        operation = extractNextOp(itr);
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = new ValidationResult();

        if (null != condition && null != conditional) {
            result.addError("Both a condition and a conditional have been provided " +
                    "- only one should be configured.");
        }

        return result;
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

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getMaxRepeats() {
        return maxRepeats;
    }

    public void setMaxRepeats(final int maxRepeats) {
        this.maxRepeats = maxRepeats;
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
                .append(getInput())
                .append(maxRepeats)
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

        final While whileOp = (While) obj;

        return new EqualsBuilder()
                .append(getInput(), whileOp.getInput())
                .append(condition, whileOp.isCondition())
                .append(conditional, whileOp.conditional)
                .append(operation, whileOp.operation)
                .append(maxRepeats, whileOp.maxRepeats)
                .append(options, whileOp.options)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append(getInput())
                .append(maxRepeats)
                .append(condition)
                .append(conditional)
                .append(operation)
                .append(options)
                .toString();
    }

    public static final class Builder<I, O>
            extends Operation.BaseBuilder<While<I, O>, Builder<I, O>>
            implements InputOutput.Builder<While<I, O>, I, O, Builder<I, O>> {

        public Builder() {
            super(new While<>());
        }

        public Builder<I, O> operation(final Operation op) {
            _getOp().setOperation(op);
            return _self();
        }

        public Builder<I, O> maxRepeats(final Integer repeats) {
            _getOp().setMaxRepeats(repeats);
            return _self();
        }

        public Builder<I, O> condition(final Boolean condition) {
            if (null != condition && null != _getOp().getConditional()) {
                throw new IllegalArgumentException("Tried to set condition when conditional has already been configured.");
            }

            _getOp().setCondition(condition);
            return _self();
        }

        public Builder<I, O> conditional(final Conditional conditional) {
            if (null != conditional && null != _getOp().isCondition()) {
                throw new IllegalArgumentException("Tried to set conditional when condition has already been configured.");
            }

            _getOp().setConditional(conditional);
            return _self();
        }

        public Builder<I, O> conditional(final Predicate predicate) {
            if (null != predicate && null != _getOp().isCondition()) {
                throw new IllegalArgumentException("Tried to set conditional when condition has already been configured.");
            }

            _getOp().setConditional(new Conditional(predicate));
            return _self();
        }

        public Builder<I, O> conditional(final Predicate predicate, final Operation transform) {
            if ((null != predicate || null != transform) && null != _getOp().isCondition()) {
                throw new IllegalArgumentException("Tried to set conditional when condition has already been configured.");
            }

            _getOp().setConditional(new Conditional(predicate, transform));
            return _self();
        }
    }
}
