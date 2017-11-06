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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * A {@code GetWalks} class is used to retrieve all of the walks in a graph
 * starting from one of a set of provided {@link EntityId}s, with a maximum
 * length.
 * <p>
 * A GetWalks operation is configured using a user-supplied list of {@link
 * GetElements} operations. These are executed sequentially, with the output of
 * one operation providing the input {@link EntityId}s for the next.
 */
public class GetWalks implements
        InputOutput<Iterable<? extends EntityId>, Iterable<Walk>>,
        MultiInput<EntityId>,
        Operations<GetElements> {

    private List<GetElements> operations;
    private Iterable<? extends EntityId> input;
    private Map<String, String> options;
    private Integer resultsLimit = 1000000;

    @Override
    public Iterable<? extends EntityId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends EntityId> input) {
        this.input = input;
    }

    @Override
    public List<GetElements> getOperations() {
        return operations;
    }

    public void setOperations(final List<GetElements> operations) {
        this.operations = operations;
    }

    public void addOperation(final GetElements operation) {
        if (null == this.operations) {
            this.operations = new ArrayList<>();
        }

        this.operations.add(operation);
    }

    @Override
    public Class<GetElements> getOperationsClass() {
        return GetElements.class;
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = InputOutput.super.validate();

        // Validate the View objects
        if (null != operations) {
            for (final ListIterator<GetElements> it = operations.listIterator(); it.hasNext();) {
                final GetElements op = it.next();

                // Validate that the input is set correctly
                if (null != op.getInput()) {
                    result.addError("The input for all the nested operations must be null.");
                }
            }
        } else {
            result.addError("No GetElements operations were provided.");
        }

        return result;
    }

    @Override
    public TypeReference<Iterable<Walk>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableWalk();
    }

    @Override
    public GetWalks shallowClone() throws CloneFailedException {
        final GetWalks.Builder builder = new GetWalks.Builder();

        builder.input(input);
        builder.options(options);

        for (final GetElements op : operations) {
            builder.operation(op.shallowClone());
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

    public Integer getResultsLimit() {
        return resultsLimit;
    }

    public void setResultsLimit(final Integer resultsLimit) {
        this.resultsLimit = resultsLimit;
    }

    public static final class Builder
            extends Operation.BaseBuilder<GetWalks, Builder>
            implements InputOutput.Builder<GetWalks, Iterable<? extends EntityId>, Iterable<Walk>, Builder>,
            MultiInput.Builder<GetWalks, EntityId, Builder> {

        public Builder() {
            super(new GetWalks());
        }

        public Builder operations(final Iterable<GetElements> operations) {
            if (null != operations) {
                _getOp().setOperations(Lists.newArrayList(operations));
            }
            return _self();
        }

        public Builder operations(final GetElements... operations) {
            _getOp().setOperations(Arrays.asList(operations));
            return _self();
        }

        public Builder operation(final GetElements operation) {
            _getOp().addOperation(operation);
            return _self();
        }

        public Builder resultsLimit(final Integer resultLimit) {
            _getOp().setResultsLimit(resultLimit);
            return _self();
        }
    }
}
