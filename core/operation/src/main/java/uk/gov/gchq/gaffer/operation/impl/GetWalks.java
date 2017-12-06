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

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.graph.Walk;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.data.WalkDefinition;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static java.util.stream.Collectors.toList;

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
        Operations<Operation> {

    private List<WalkDefinition> walkDefinitions;
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

    public List<WalkDefinition> getWalkDefinitions() {
        return walkDefinitions;
    }

    public void setWalkDefinitions(final List<WalkDefinition> walkDefinitions) {
        this.walkDefinitions = walkDefinitions;
    }

    public void addWalkDefinition(final WalkDefinition walkDefinition) {
        if (null == this.walkDefinitions) {
            this.walkDefinitions = new ArrayList<>();
        }

        this.walkDefinitions.add(walkDefinition);
    }

    @Override
    public ValidationResult validate() {
        final ValidationResult result = InputOutput.super.validate();

        // Validate the View objects
        if (null != walkDefinitions) {
            for (final ListIterator<WalkDefinition> it = walkDefinitions.listIterator(); it.hasNext();) {
                final WalkDefinition walkDefinition = it.next();
                for (final Operation preOp : walkDefinition.getPreFilters().getOperations()) {
                    if (!Input.class.isAssignableFrom(preOp.getClass())) {
                        result.addError("The pre operation filter " + preOp.getClass().getCanonicalName() +
                                " does not accept an input.");
                    }
                }

                for (final Operation postOp : walkDefinition.getPostFilters().getOperations()) {
                    if (!postOp.getClass().isAssignableFrom(Input.class)) {
                        result.addError("The post operation filter " + postOp.getClass().getCanonicalName() +
                                " does not accept an input.");
                    }
                }

                final GetElements op = walkDefinition.getOperation();

                // Validate that the input is set correctly
                if (null != op.getInput()) {
                    result.addError("The input for all the nested operations must be null.");
                }

                final View view = op.getView();

                if (null != view) {
                     if (!view.hasEntities() && !view.hasEdges()) {
                         result.addError("The view must contain either edge or entity definitions.");
                     }
                } else {
                    result.addError("The view must not be null.");
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
    public GetWalks shallowClone() {
        final GetWalks.Builder builder = new GetWalks.Builder();

        builder.input(input);
        builder.options(options);

        if (null != walkDefinitions) {
            for (final WalkDefinition definition : walkDefinitions) {
                builder.walkDefinition(definition.clone());
            }
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

    @Override
    public Collection<Operation> getOperations() {
        return walkDefinitions.stream()
                .map(WalkDefinition::asList)
                .flatMap(List::stream)
                .collect(toList());
    }

    public static final class Builder
            extends Operation.BaseBuilder<GetWalks, Builder>
            implements InputOutput.Builder<GetWalks, Iterable<? extends EntityId>, Iterable<Walk>, Builder>,
            MultiInput.Builder<GetWalks, EntityId, Builder> {

        public Builder() {
            super(new GetWalks());
        }

        public Builder walkDefinitions(final Iterable<WalkDefinition> walkDefinitions) {
            if (null != walkDefinitions) {
                _getOp().setWalkDefinitions(Lists.newArrayList(walkDefinitions));
            }
            return _self();
        }

        public Builder walkDefinitions(final WalkDefinition... walkDefinitions) {
            _getOp().setWalkDefinitions(Arrays.asList(walkDefinitions));
            return _self();
        }

        public Builder walkDefinition(final WalkDefinition walkDefinition) {
            _getOp().addWalkDefinition(walkDefinition);
            return _self();
        }

        public Builder resultsLimit(final Integer resultLimit) {
            _getOp().setResultsLimit(resultLimit);
            return _self();
        }
    }
}
