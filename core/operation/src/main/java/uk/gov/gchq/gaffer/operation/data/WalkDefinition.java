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

package uk.gov.gchq.gaffer.operation.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;

import java.util.List;

/**
 * A {@code WalkDefinition} describes how to carry out a single step in a {@link uk.gov.gchq.gaffer.data.graph.Walk}.
 */
public class WalkDefinition implements Cloneable {

    private final OperationChain<? extends Iterable<ElementId>> preFilters;
    private final OperationChain<? extends Iterable<ElementId>> postFilters;
    private final GetElements operation;

    public WalkDefinition(final Builder builder) {
        this.preFilters = builder.preFilters;
        this.postFilters = builder.postFilters;
        this.operation = builder.operation;
    }

    /**
     * Public constructor used by Jackson.
     *
     * @param preFilters  the preFilter operation chain
     * @param postFilters the postFilter operation chain
     * @param operation   the GetElements operation
     */
    @JsonCreator
    public WalkDefinition(@JsonProperty("preFilters") final OperationChain<? extends Iterable<ElementId>> preFilters,
                          @JsonProperty("postFilters") final OperationChain<? extends Iterable<ElementId>> postFilters,
                          @JsonProperty("operation") final GetElements operation) {
        this.preFilters = preFilters;
        this.postFilters = postFilters;
        this.operation = operation;
    }

    public OperationChain<? extends Iterable<? extends ElementId>> getPostFilters() {
        return postFilters;
    }

    public GetElements getOperation() {
        return operation;
    }

    public OperationChain<? extends Iterable<ElementId>> getPreFilters() {
        return preFilters;
    }

    @JsonIgnore
    public List<Operation> getPreFiltersList() {
        return preFilters.getOperations();
    }

    @JsonIgnore
    public List<Operation> getPostFiltersList() {
        return postFilters.getOperations();
    }

    @Override
    public WalkDefinition clone() {
        final WalkDefinition.Builder builder = new WalkDefinition.Builder();

        builder.postFilters = postFilters.shallowClone();
        builder.preFilters = preFilters.shallowClone();
        builder.operation = operation.shallowClone();

        return builder.build();
    }

    public static class Builder {
        private OperationChain<? extends Iterable<ElementId>> preFilters;
        private OperationChain<? extends Iterable<ElementId>> postFilters;

        private OperationChain.OutputBuilder<? extends Iterable<ElementId>> preFiltersBuilder;
        private OperationChain.OutputBuilder<? extends Iterable<ElementId>> postFiltersBuilder;

        private GetElements operation;

        public Builder() {
        }

        public Builder preFilter(final Operation operation) {
            if (null == preFiltersBuilder) {
                preFiltersBuilder = new OperationChain.Builder().first((Output) operation);
            } else {
                preFiltersBuilder.then((Input) operation);
            }
            return this;
        }

        public Builder postFilter(final Operation operation) {
            if (null == postFiltersBuilder) {
                postFiltersBuilder = new OperationChain.Builder().first((Output) operation);
            } else {
                postFiltersBuilder.then((Input) operation);
            }
            return this;
        }

        public Builder operation(final GetElements operation) {
            this.operation = operation;
            return this;
        }

        public WalkDefinition build() {
            if (null == preFilters) {
                if (null != preFiltersBuilder) {
                    preFilters = preFiltersBuilder.build();
                } else {
                    preFilters = new OperationChain<>();
                }
            }

            if (null == postFilters) {
                if (null != postFiltersBuilder) {
                    postFilters = postFiltersBuilder.build();
                } else {
                    postFilters = new OperationChain<>();
                }
            }
            return new WalkDefinition(this);
        }
    }

}
