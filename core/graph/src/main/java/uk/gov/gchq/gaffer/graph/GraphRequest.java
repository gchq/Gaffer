/*
 * Copyright 2018 Crown Copyright
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
package uk.gov.gchq.gaffer.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

/**
 * A {@code GraphRequest} is a request that will be executed on a Gaffer {@link Graph}.
 * A new {@link Context} with new jobId will be created based on your {@link Context}/{@link User}.
 *
 * @param <O> the result type of the request.
 */
public class GraphRequest<O> {
    private final OperationChain<O> operationChain;
    private final Context context;

    public GraphRequest(final Operation operation, final User user) {
        if (null == operation) {
            throw new IllegalArgumentException("An operation is required");
        }
        if (null == user) {
            throw new IllegalArgumentException("A user is required");
        }
        this.operationChain = (OperationChain) OperationChain.wrap(operation);
        this.context = new Context(user);
    }

    public GraphRequest(final Output<O> operation, final User user) {
        if (null == operation) {
            throw new IllegalArgumentException("An operation is required");
        }
        if (null == user) {
            throw new IllegalArgumentException("A user is required");
        }
        this.operationChain = OperationChain.wrap(operation);
        this.context = new Context(user);
    }

    public GraphRequest(final Operation operation, final Context context) {
        if (null == operation) {
            throw new IllegalArgumentException("An operation is required");
        }
        if (null == context) {
            throw new IllegalArgumentException("A context containing a user is required");
        }
        this.operationChain = (OperationChain) OperationChain.wrap(operation);
        this.context = context;
    }

    @JsonCreator
    public GraphRequest(@JsonProperty("operationChain") final Output<O> operation, @JsonProperty("context") final Context context) {
        if (null == operation) {
            throw new IllegalArgumentException("An operation is required");
        }
        if (null == context) {
            throw new IllegalArgumentException("A context containing a user is required");
        }
        this.operationChain = OperationChain.wrap(operation);
        this.context = context;
    }

    public OperationChain<O> getOperationChain() {
        return operationChain;
    }

    public Context getContext() {
        return context;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GraphRequest<?> that = (GraphRequest<?>) o;

        return new EqualsBuilder()
                .append(operationChain, that.operationChain)
                .append(context, that.context)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(operationChain)
                .append(context)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("operationChain", operationChain)
                .append("context", context)
                .toString();
    }


}
