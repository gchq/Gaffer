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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * An {@code FederatedOperationChain} holds an {@link OperationChain} that will
 * be executed in one go on the federated graphs.
 * </p>
 *
 * @param <O_ITEM> the output iterable type of the {@code FederatedOperationChain}.
 **/
public class FederatedOperationChain<O_ITEM> implements Output<CloseableIterable<O_ITEM>>,
        Operations<OperationChain> {
    @Required
    private OperationChain operationChain;
    private Map<String, String> options;

    public FederatedOperationChain() {
        this(new OperationChain());
    }

    public FederatedOperationChain(final Operation... operations) {
        this(new OperationChain(operations));
    }

    public FederatedOperationChain(final OperationChain operationChain) {
        setOperationChain(operationChain);
    }

    @JsonCreator
    public FederatedOperationChain(@JsonProperty("operationChain") final OperationChainDAO operationChain) {
        this((OperationChain) operationChain);
    }

    @Override
    public TypeReference<CloseableIterable<O_ITEM>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.CloseableIterableObj();
    }

    public OperationChain getOperationChain() {
        return operationChain;
    }

    @JsonGetter("operationChain")
    OperationChainDAO getOperationChainDao() {
        if (operationChain instanceof OperationChainDAO) {
            return (OperationChainDAO) operationChain;
        }

        return new OperationChainDAO(operationChain);
    }

    @JsonIgnore
    @Override
    public List<OperationChain> getOperations() {
        return Collections.singletonList(operationChain);
    }

    public FederatedOperationChain<O_ITEM> shallowClone() throws CloneFailedException {
        return new FederatedOperationChain.Builder<O_ITEM>()
                .operationChain(operationChain.shallowClone())
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

    private void setOperationChain(final OperationChain operationChain) {
        if (null == operationChain) {
            throw new IllegalArgumentException("operationChain is required");
        }
        this.operationChain = operationChain;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("operationChain", operationChain)
                .build();
    }

    @Override
    public void close() throws IOException {
        operationChain.close();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final FederatedOperationChain<?> federatedOperationChain = (FederatedOperationChain<?>) obj;

        return new EqualsBuilder()
                .append(operationChain, federatedOperationChain.operationChain)
                .append(options, federatedOperationChain.options)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 23)
                .append(operationChain)
                .append(options)
                .toHashCode();
    }

    public static class Builder<O_ITEM> extends
            Operation.BaseBuilder<FederatedOperationChain<O_ITEM>, Builder<O_ITEM>>
            implements Output.Builder<FederatedOperationChain<O_ITEM>, CloseableIterable<O_ITEM>, Builder<O_ITEM>> {
        public Builder() {
            super(new FederatedOperationChain<>(new OperationChain()));
        }

        public Builder<O_ITEM> operationChain(final OperationChain operationChain) {
            _getOp().setOperationChain(operationChain);
            return this;
        }
    }
}
