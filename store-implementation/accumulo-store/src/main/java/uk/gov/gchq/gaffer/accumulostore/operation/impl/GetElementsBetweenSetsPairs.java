/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * Given a {@link Pair} of sets of {@link uk.gov.gchq.gaffer.data.element.id.EntityId}s,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in the first
 * set and the other end is in the second set. Also returns
 * {@link uk.gov.gchq.gaffer.data.element.Entity}s for
 * {@link uk.gov.gchq.gaffer.data.element.id.EntityId}s in the first set.
 */
@JsonPropertyOrder(value = {"class", "input", "view"}, alphabetic = true)
@Since("2.0.0")
@Summary("Gets edges that exist between 2 sets and entities in the first set")
public class GetElementsBetweenSetsPairs implements
        InputOutput<Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>>, Iterable<? extends Element>>,
        SeededGraphFilters {

    private View view;
    private IncludeIncomingOutgoingType includeIncomingOutGoing;
    private DirectedType directedType;
    private Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>> input;
    private Map<String, String> options;

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return includeIncomingOutGoing;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType inOutType) {
        this.includeIncomingOutGoing = inOutType;
    }

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    @Override
    public DirectedType getDirectedType() {
        return directedType;
    }

    @Override
    public void setDirectedType(final DirectedType directedType) {
        this.directedType = directedType;
    }

    @Override
    public Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>> getInput() {
        return input;
    }

    @Override
    public void setInput(final Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
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
    public GetElementsBetweenSetsPairs shallowClone() {
        return new GetElementsBetweenSetsPairs.Builder()
                .view(view)
                .inOutType(includeIncomingOutGoing)
                .directedType(directedType)
                .input(input)
                .options(options)
                .build();
    }

    public GetElementsBetweenSets getBackwardsCompatibleOperation() {
        return new GetElementsBetweenSets.Builder()
                .input(getInput() != null ? getInput().getFirst() : null)
                .inputB(getInput() != null ? getInput().getSecond() : null)
                .view(getView())
                .directedType(getDirectedType())
                .inOutType(getIncludeIncomingOutGoing())
                .options(getOptions())
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetElementsBetweenSetsPairs, Builder>
            implements
            Output.Builder<GetElementsBetweenSetsPairs, Iterable<? extends Element>, Builder>,
            SeededGraphFilters.Builder<GetElementsBetweenSetsPairs, Builder> {
        public Builder() {
            super(new GetElementsBetweenSetsPairs());
        }

        public Builder input(final Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>> input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            _getOp().setInput(input);
            return _self();
        }

        public Builder input(final Object... input) {
            return input(Lists.newArrayList(input));
        }

        public Builder input(final Iterable input) {
            _getOp().setInput(new Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>>(OperationUtil.toEntityIds(input)));
            return _self();
        }

        public Builder input(final EntityId... input) {
            return inputIds(Lists.newArrayList(input));
        }

        public Builder inputIds(final Iterable<? extends EntityId> input) {
            _getOp().setInput(new Pair<Iterable<? extends EntityId>, Iterable<? extends EntityId>>(input));
            return _self();
        }

        public Builder inputB(final Object... input) {
            return inputB(Lists.newArrayList(input));
        }

        public Builder inputB(final Iterable input) {
            _getOp().getInput().setSecond(OperationUtil.toEntityIds(input));
            return _self();
        }

        public Builder inputB(final EntityId... input) {
            return inputBIds(Lists.newArrayList(input));
        }

        public Builder inputBIds(final Iterable<? extends EntityId> input) {
            _getOp().getInput().setSecond(input);
            return _self();
        }
    }
}
