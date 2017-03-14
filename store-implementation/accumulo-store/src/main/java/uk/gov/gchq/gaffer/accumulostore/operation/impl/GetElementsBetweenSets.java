/*
 * Copyright 2016 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableInput;
import uk.gov.gchq.gaffer.operation.IterableInputB;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Map;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link uk.gov.gchq.gaffer.data.element.Entity}s for
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s in set A.
 */
public class GetElementsBetweenSets<E extends Element> implements
        Operation,
        IterableInput<EntitySeed>,
        IterableInputB<EntitySeed>,
        IterableOutput<E>,
        SeededGraphFilters,
        SeedMatching,
        Options {
    private SeedMatchingType seedMatching = SeedMatchingType.RELATED;
    private View view;
    private IncludeIncomingOutgoingType inOutType;
    private DirectedType directedType;
    private Iterable<EntitySeed> input;
    private Iterable<EntitySeed> inputB;
    private Map<String, String> options;

    /**
     * @param seedMatching a {@link SeedMatchingType} describing how the seeds should be
     *                     matched to the identifiers in the graph.
     * @see SeedMatchingType
     */
    public void setSeedMatching(final SeedMatchingType seedMatching) {
        this.seedMatching = seedMatching;
    }

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
    public Iterable<EntitySeed> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<EntitySeed> input) {
        this.input = input;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @Override
    public Object[] createInputArray() {
        return IterableInput.super.createInputArray();
    }

    @Override
    public TypeReference<CloseableIterable<E>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.CloseableIterableElement();
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
    public Iterable<EntitySeed> getInputB() {
        return inputB;
    }

    @Override
    public void setInputB(final Iterable<EntitySeed> inputB) {
        this.inputB = inputB;
    }

    public static class Builder<E extends Element> extends Operation.BaseBuilder<GetElementsBetweenSets<E>, Builder<E>>
            implements IterableInput.Builder<GetElementsBetweenSets<E>, EntitySeed, Builder<E>>,
            IterableInputB.Builder<GetElementsBetweenSets<E>, EntitySeed, Builder<E>>,
            IterableOutput.Builder<GetElementsBetweenSets<E>, E, Builder<E>>,
            SeededGraphFilters.Builder<GetElementsBetweenSets<E>, Builder<E>>,
            SeedMatching.Builder<GetElementsBetweenSets<E>, Builder<E>>,
            Options.Builder<GetElementsBetweenSets<E>, Builder<E>> {
        public Builder() {
            super(new GetElementsBetweenSets<>());
        }
    }
}
