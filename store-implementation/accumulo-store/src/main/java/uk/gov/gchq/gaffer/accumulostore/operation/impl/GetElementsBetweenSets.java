/*
 * Copyright 2016-2018 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.accumulostore.operation.MultiEntityIdInputB;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.io.IOException;
import java.util.Map;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.data.element.id.EntityId}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link uk.gov.gchq.gaffer.data.element.Entity}s for
 * {@link uk.gov.gchq.gaffer.data.element.id.EntityId}s in set A.
 */
@JsonPropertyOrder(value = {"class", "input", "inputB", "view"}, alphabetic = true)
@Since("1.0.0")
@Summary("Gets edges that exist between 2 sets and entities in the first set")
public class GetElementsBetweenSets implements
        InputOutput<Iterable<? extends EntityId>, CloseableIterable<? extends Element>>,
        MultiEntityIdInput,
        MultiEntityIdInputB,
        SeededGraphFilters,
        SeedMatching {

    /**
     * @deprecated use a {@link View} instead to specify whether
     * Edges/Entities that are 'equal to' or 'related to' seeds are wanted.
     */
    private SeedMatchingType seedMatching;

    private View view;
    private IncludeIncomingOutgoingType inOutType;
    private DirectedType directedType;
    private Iterable<? extends EntityId> input;
    private Iterable<? extends EntityId> inputB;
    private Map<String, String> options;

    /**
     * @param seedMatching a {@link SeedMatchingType} describing how the seeds should be
     *                     matched to the identifiers in the graph.
     * @see SeedMatchingType
     */
    @Override
    public void setSeedMatching(final SeedMatchingType seedMatching) {
        this.seedMatching = seedMatching;
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return seedMatching;
    }

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return inOutType;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType inOutType) {
        this.inOutType = inOutType;
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
    public Iterable<? extends EntityId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends EntityId> input) {
        this.input = input;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @Override
    public Object[] createInputBArray() {
        return MultiEntityIdInputB.super.createInputBArray();
    }

    @Override
    public TypeReference<CloseableIterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
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
    public Iterable<? extends EntityId> getInputB() {
        return inputB;
    }

    @Override
    public void setInputB(final Iterable<? extends EntityId> inputB) {
        this.inputB = inputB;
    }

    @Override
    public void close() throws IOException {
        MultiEntityIdInput.super.close();
        CloseableUtil.close(inputB);
    }

    @Override
    public GetElementsBetweenSets shallowClone() {
        return new GetElementsBetweenSets.Builder()
                .seedMatching(seedMatching)
                .view(view)
                .inOutType(inOutType)
                .directedType(directedType)
                .input(input)
                .inputB(inputB)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetElementsBetweenSets, Builder>
            implements InputOutput.Builder<GetElementsBetweenSets, Iterable<? extends EntityId>, CloseableIterable<? extends Element>, Builder>,
            MultiEntityIdInput.Builder<GetElementsBetweenSets, Builder>,
            MultiEntityIdInputB.Builder<GetElementsBetweenSets, Builder>,
            SeededGraphFilters.Builder<GetElementsBetweenSets, Builder>,
            SeedMatching.Builder<GetElementsBetweenSets, Builder> {
        public Builder() {
            super(new GetElementsBetweenSets());
        }
    }
}
