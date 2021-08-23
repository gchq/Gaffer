/*
 * Copyright 2016-2021 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.util.CloseableUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

/**
 * <p>
 * An {@code OperationChain} holds a list of
 * {@link Operation}s that are chained together -
 * ie. the output of one operation is passed to the input of the next. For the chaining to be successful the operations
 * must be ordered correctly so the O and I types are compatible. The safest way to ensure they will be
 * compatible is to use the OperationChain.Builder to construct the chain.
 * </p>
 * IBuilderId couple of special cases:
 * <ul>
 * <li>An operation with no output can come before any operation.</li>
 * <li>An operation with no input can follow any operation - the output from the previous operation will
 * just be lost.</li>
 * </ul>
 */
@JsonPropertyOrder(value = {"class", "id", "operations"}, alphabetic = true)
@Since("0.0.1")
@Summary("IBuilderId chain of operations where the results are passed between each operation")
public class OperationChain extends Operation implements Operations {
    private List<Operation> operations;

    public OperationChain(final String id, final Map<String, Object> operationArgs) {
        this(id, operationArgs, (List<Operation>) null);
    }

    public OperationChain(final String id, final Map<String, Object> operationArgs, final Operation operation) {
        this(id, operationArgs, isNull(operation) ? null : Lists.newArrayList(operation));
    }

    @JsonCreator
    public OperationChain(@JsonProperty("id") final String id, @JsonProperty("operationArgs") final Map<String, Object> operationArgs, @JsonProperty("operations") final Operation... operations) {
        this(id, operationArgs, isNull(operations) ? null : Lists.newArrayList(operations));
    }

    public OperationChain(final String id, final Map<String, Object> operationArgs, final List<Operation> operations) {
        this(id, operationArgs, operations, false);
    }

    public OperationChain(final String id, final Map<String, Object> operationArgs, final List<Operation> operations, final boolean flatten) {
        super(id, operationArgs);
        // super((id.toLowerCase(LOCALE).endsWith("chain") ? id : id + "Chain"));
        if (isNull(operations)) {
            this.operations = new ArrayList<>();
        } else {
            this.operations = new ArrayList<>(operations);
        }

        this.operations.removeIf(Objects::isNull);

        if (flatten) {
            this.operations = flatten();
        }
    }

    public static OperationChain wrap(final String id, final Operation operation) {
        Objects.requireNonNull(operation);
        final OperationChain opChain;
        if (operation instanceof OperationChain) {
            opChain = ((OperationChain) operation);
        } else {
            opChain = new OperationChain(id, operation.getOperationArgs(), operation); //TODO review the wrapping of operations in operation chains.
        }
        return opChain;
    }

    @JsonIgnore
    public TypeReference getOutputTypeReference() {
        TypeReference rtn = new TypeReferenceImpl.Void();
        if (!operations.isEmpty()) {
            final Operation lastOp = operations.get(operations.size() - 1);
            rtn = (TypeReference) lastOp.get(OperationConstants.KEY_OUTPUT_TYPE_REFERENCE);
        }

        return rtn;
    }

    @JsonIgnore
    @Override
    public List<Operation> getOperations() {
        return operations;
    }

    @JsonGetter("operations")
    Operation[] getOperationArray() {
        return operations.toArray(new Operation[operations.size()]);
    }

    @JsonSetter("operations")
    void setOperationArray(final Operation[] operations) {
        if (null != operations) {
            this.operations = Lists.newArrayList(operations);
        } else {
            this.operations = new ArrayList<>();
        }
    }


    @Override
    public OperationChain addOperationArgs(final Map<String, Object> operationsArgs) {
        return (OperationChain) super.addOperationArgs(operationsArgs);
    }


    public static OperationChain cast(final Operation operation) {
        return new OperationChain(operation.getId(), operation.getOperationArgs());
    }

    public OperationChain shallowClone() throws CloneFailedException {
        final OperationChain clone = OperationChain.cast(super.shallowClone());
        clone.getOperations().addAll(operations);
        return clone;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("operations", operations)
                .build();
    }

    public String toOverviewString() {
        final String opStrings = operations.stream()
                .filter(o -> null != o)
                .map(o -> o.getClass().getSimpleName())
                .collect(Collectors.joining("->"));

        return getClass().getSimpleName() + "[" + opStrings + "]";
    }

    @Override
    public void close() throws IOException {
        for (final Operation operation : operations) {
            CloseableUtil.close(operation);
        }
    }

    @Override
    public boolean equals(final Object obj) {
        boolean isEqual = false;
        if (null != obj && obj instanceof OperationChain) {
            final OperationChain that = (OperationChain) obj;

            isEqual = new EqualsBuilder()
                    .appendSuper(super.equals(obj))
                    .append(this.getOperations(), that.getOperations())
                    .isEquals();
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 21)
                .appendSuper(super.hashCode())
                .append(operations)
                .toHashCode();
    }
}
