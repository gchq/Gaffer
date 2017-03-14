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
 * <li>An operation with no output can come before any operation.</li>
 * <li>An operation with no input can follow any operation - the output from the previous operation will
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

    public OperationChain(final Operation operation) {
        this(new ArrayList<>(1));
        operations.add(operation);
    }

    public OperationChain(final List<Operation> operations) {
        this.operations = new ArrayList<>(operations);
    }

    @JsonIgnore
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

    public static class Builder {
        private final List<Operation> ops = new ArrayList<>();

        public Builder first(final Operation op) {
            ops.add(op);
            return this;
        }

        public Builder then(final Operation op) {
            ops.add(op);
            return this;
        }

        public <T> OperationChain<T> build() {
            return new OperationChain<>(ops);
        }
    }
}
