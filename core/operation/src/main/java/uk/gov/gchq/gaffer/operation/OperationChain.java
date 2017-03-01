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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.impl.export.Export;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.ArrayList;
import java.util.List;

/**
 * An <code>OperationChain</code> holds a list of {@link uk.gov.gchq.gaffer.operation.Operation}s that are chained together -
 * ie. the output of one operation is passed to the input of the next. For the chaining to be successful the operations
 * must be ordered correctly so the OUTPUT and INPUT types are compatible. The safest way to ensure they will be
 * compatible is to use the OperationChain.Builder to construct the chain.
 * <p>
 * A couple of special cases:
 * <ul>
 * <li>A {@link uk.gov.gchq.gaffer.operation.VoidOutput} can come before any operation - as long as the following operation doesn't
 * require some seeds to be automatically set.</li>
 * <li>A {@link uk.gov.gchq.gaffer.operation.VoidInput} can follow any operation - the output from the previous operation will
 * just be lost.</li>
 * </ul>
 *
 * @param <OUT> the output type of the <code>OperationChain</code>. This should match the output type of the last
 *              {@link uk.gov.gchq.gaffer.operation.Operation} in the chain.
 * @see uk.gov.gchq.gaffer.operation.OperationChain.Builder
 */
public class OperationChain<OUT> {
    private List<Operation> operations;

    public OperationChain() {
    }

    public OperationChain(final Operation<?, OUT> operation) {
        this(new ArrayList<>(1));
        operations.add(operation);
    }

    public OperationChain(final List<Operation> operations) {
        this.operations = new ArrayList<>(operations);
    }

    @JsonIgnore
    public TypeReference<OUT> getOutputTypeReference() {
        if (null == operations || operations.isEmpty()) {
            return (TypeReference<OUT>) new TypeReferenceImpl.Object();
        }

        return operations.get(operations.size() - 1).getOutputTypeReference();
    }

    public List<Operation> getOperations() {
        return operations;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("operations")
    Operation[] getOperationArray() {
        return null != operations ? operations.toArray(new Operation[operations.size()]) : new Operation[0];
    }

    @JsonSetter("operations")
    void setOperationArray(final Operation[] operations) {
        if (null != operations) {
            this.operations = Lists.newArrayList(operations);
        } else {
            this.operations = null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder strBuilder = new StringBuilder("OperationChain[");

        if (null != operations) {
            boolean first = true;
            for (final Operation op : operations) {
                if (first) {
                    first = false;
                } else {
                    strBuilder.append("->");
                }

                strBuilder.append(op.getClass().getSimpleName());
            }
        }

        strBuilder.append("]");
        return strBuilder.toString();
    }

    /**
     * A <code>Builder</code> is a type safe way of building an {@link uk.gov.gchq.gaffer.operation.OperationChain}.
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
     * <p>
     * For a full example see the Example module.
     */
    public static class Builder {
        public <NEXT_OUT> TypedBuilder<NEXT_OUT> first(final Operation<?, NEXT_OUT> op) {
            return new TypedBuilder<>(op);
        }

        public TypelessBuilder first(final VoidOutput<?> op) {
            return new TypelessBuilder(op);
        }

        public TypelessBuilder first(final Export op) {
            return new TypelessBuilder(op);
        }
    }

    public static final class TypelessBuilder {
        private final List<Operation> ops;

        private TypelessBuilder(final Operation op) {
            this(new ArrayList<>());
            ops.add(op);
        }

        private TypelessBuilder(final List<Operation> ops) {
            this.ops = ops;
        }

        public <NEXT_OUT> TypedBuilder<NEXT_OUT> then(final Operation<?, NEXT_OUT> op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public <NEXT_OUT> TypedBuilder<NEXT_OUT> then(final VoidInput<NEXT_OUT> op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public TypelessBuilder then(final Export op) {
            ops.add(op);
            return new TypelessBuilder(ops);
        }

        public TypedBuilder<JobDetail> then(final GetJobDetails op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public TypedBuilder<CloseableIterable<JobDetail>> then(final GetAllJobDetails op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public TypelessBuilder then(final VoidOutput<?> op) {
            ops.add(op);
            return new TypelessBuilder(ops);
        }

        public OperationChain<Void> build() {
            return new OperationChain<>(ops);
        }
    }

    public static final class TypedBuilder<OUT> {
        private final List<Operation> ops;

        private TypedBuilder(final Operation<?, OUT> op) {
            this(new ArrayList<>());
            ops.add(op);
        }

        private TypedBuilder(final List<Operation> ops) {
            this.ops = ops;
        }


        public TypelessBuilder then(final Export op) {
            ops.add(op);
            return new TypelessBuilder(ops);
        }

        public TypedBuilder<JobDetail> then(final GetJobDetails op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public TypedBuilder<CloseableIterable<JobDetail>> then(final GetAllJobDetails op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public <NEXT_OUT> TypedBuilder<NEXT_OUT> then(final Operation<? extends OUT, NEXT_OUT> op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public <NEXT_OUT> TypedBuilder<NEXT_OUT> then(final VoidInput<NEXT_OUT> op) {
            ops.add(op);
            return new TypedBuilder<>(ops);
        }

        public TypelessBuilder then(final VoidOutput<OUT> op) {
            ops.add(op);
            return new TypelessBuilder(ops);
        }

        public OperationChain<OUT> build() {
            return new OperationChain<>(ops);
        }
    }
}
