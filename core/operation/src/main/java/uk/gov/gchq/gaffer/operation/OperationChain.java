/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 * An {@code OperationChain} holds a list of {@link uk.gov.gchq.gaffer.operation.Operation}s that are chained together -
 * ie. the output of one operation is passed to the input of the next. For the chaining to be successful the operations
 * must be ordered correctly so the O and I types are compatible. The safest way to ensure they will be
 * compatible is to use the OperationChain.Builder to construct the chain.
 * </p>
 * A couple of special cases:
 * <ul>
 * <li>An operation with no output can come before any operation.</li>
 * <li>An operation with no input can follow any operation - the output from the previous operation will
 * just be lost.</li>
 * </ul>
 *
 * @param <OUT> the output type of the {@code OperationChain}. This should match the output type of the last
 *              {@link uk.gov.gchq.gaffer.operation.Operation} in the chain.
 * @see uk.gov.gchq.gaffer.operation.OperationChain.Builder
 */
public class OperationChain<OUT> implements Output<OUT> {
    private List<Operation> operations;
    private Map<String, String> options;

    public OperationChain() {
        this(new ArrayList<>());
    }

    public OperationChain(final Operation operation) {
        this(new ArrayList<>(1));
        operations.add(operation);
    }

    public OperationChain(final Output<OUT> operation) {
        this(new ArrayList<>(1));
        operations.add(operation);
    }

    public OperationChain(final Operation... operations) {
        this(new ArrayList<>(operations.length));
        for (final Operation operation : operations) {
            this.operations.add(operation);
        }
    }

    public OperationChain(final List<Operation> operations) {
        this(operations, false);
    }

    public OperationChain(final List<Operation> operations, final boolean flatten) {
        if (null == operations) {
            this.operations = new ArrayList<>();
        } else {
            this.operations = new ArrayList<>(operations);
        }

        if (flatten) {
            this.operations = flatten();
        }
    }

    public static OperationChain<?> wrap(final Operation operation) {
        if (operation instanceof OperationChain) {
            return ((OperationChain) operation);
        }

        return new OperationChain<>(operation);
    }

    public static <O> OperationChain<O> wrap(final Output<O> operation) {
        if (operation instanceof OperationChain) {
            return ((OperationChain<O>) operation);
        }

        return new OperationChain<>(operation);
    }

    @JsonIgnore
    @Override
    public TypeReference<OUT> getOutputTypeReference() {
        if (null != operations && !operations.isEmpty()) {
            final Operation lastOp = operations.get(operations.size() - 1);
            if (lastOp instanceof Output) {
                return ((Output) lastOp).getOutputTypeReference();
            }
        }

        return (TypeReference<OUT>) new TypeReferenceImpl.Void();
    }

    public List<Operation> getOperations() {
        return operations;
    }

    @JsonGetter("operations")
    Operation[] getOperationArray() {
        return null != operations ? operations.toArray(new Operation[operations.size()]) : new Operation[0];
    }

    @JsonSetter("operations")
    void setOperationArray(final Operation[] operations) {
        if (null != operations) {
            this.operations = Lists.newArrayList(operations);
        } else {
            this.operations = new ArrayList<>();
        }
    }

    public OperationChain<OUT> shallowClone() throws CloneFailedException {
        if (null == operations) {
            return new OperationChain<>();
        }

        final List<Operation> clonedOps = operations.stream()
                .map(Operation::shallowClone)
                .collect(Collectors.toList());
        final OperationChain<OUT> clone = new OperationChain<>(clonedOps);
        clone.setOptions(options);
        return clone;
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
    public String toString() {
        return new ToStringBuilder(this)
                .append("operations", operations)
                .build();
    }

    @Override
    public void close() throws IOException {
        if (null != operations) {
            for (final Operation operation : operations) {
                CloseableUtil.close(operation);
            }
        }
    }

    @Override
    public boolean equals(final Object obj) {
        boolean isEqual = false;
        if (null != obj && obj instanceof OperationChain) {
            final OperationChain that = (OperationChain) obj;

            isEqual = new EqualsBuilder()
                    .append(this.getOperations(), that.getOperations())
                    .isEquals();
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 21)
                .append(operations)
                .toHashCode();
    }

    public List<Operation> flatten() {
        final List<Operation> tmp = new ArrayList<>(1);

        for (final Operation operation : getOperations()) {
            if (operation instanceof OperationChain) {
                tmp.addAll(((OperationChain) operation).flatten());
            } else {
                tmp.add(operation);
            }
        }

        return Collections.unmodifiableList(tmp);
    }

    /**
     * <p>
     * A {@code Builder} is a type safe way of building an {@link uk.gov.gchq.gaffer.operation.OperationChain}.
     * The builder instance is updated after each method call so it is best to chain the method calls together.
     * Usage:<br>
     * new Builder()<br>
     * &nbsp;.first(new SomeOperation.Builder()<br>
     * &nbsp;&nbsp;.addSomething()<br>
     * &nbsp;&nbsp;.build()<br>
     * &nbsp;)<br>
     * &nbsp;.then(new SomeOtherOperation.Builder()<br>
     * &nbsp;&nbsp;.addSomethingElse()<br>
     * &nbsp;&nbsp;.build()<br>
     * &nbsp;)<br>
     * &nbsp;.build();
     * </p>
     * For a full example see the Example module.
     */
    public static class Builder {
        public NoOutputBuilder first(final Operation op) {
            return new NoOutputBuilder(op);
        }

        public <NEXT_OUT> OutputBuilder<NEXT_OUT> first(final Output<NEXT_OUT> op) {
            return new OutputBuilder<>(op);
        }
    }

    public static final class NoOutputBuilder {
        private final List<Operation> ops;

        private NoOutputBuilder(final Operation op) {
            this(new ArrayList<>());
            ops.add(op);
        }

        private NoOutputBuilder(final List<Operation> ops) {
            this.ops = ops;
        }

        public NoOutputBuilder then(final Operation op) {
            ops.add(op);
            return new NoOutputBuilder(ops);
        }

        public <NEXT_OUT> OutputBuilder<NEXT_OUT> then(final Output<NEXT_OUT> op) {
            ops.add(op);
            return new OutputBuilder<>(ops);
        }

        public OperationChain<Void> build() {
            return new OperationChain<>(ops);
        }
    }

    public static final class OutputBuilder<OUT> {
        private final List<Operation> ops;

        private OutputBuilder(final Output<OUT> op) {
            this(new ArrayList<>());
            ops.add(op);
        }

        private OutputBuilder(final List<Operation> ops) {
            this.ops = ops;
        }

        public NoOutputBuilder then(final Input<? super OUT> op) {
            ops.add(op);
            return new NoOutputBuilder(ops);
        }

        public <NEXT_OUT> OutputBuilder<NEXT_OUT> then(final InputOutput<? super OUT, NEXT_OUT> op) {
            ops.add(op);
            return new OutputBuilder<>(ops);
        }

        public OperationChain<OUT> build() {
            return new OperationChain<>(ops);
        }
    }
}
