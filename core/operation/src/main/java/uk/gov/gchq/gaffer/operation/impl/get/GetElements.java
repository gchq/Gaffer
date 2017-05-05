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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Map;

/**
 * Gets elements from Gaffer based on {@link ElementId}s as
 * seeds and returns {@link uk.gov.gchq.gaffer.data.element.Element}s
 * There are various flags to filter out the elements returned.
 * See extensions of {@link GetElements} for further details.
 */
public class GetElements implements
        Operation,
        InputOutput<Iterable<? extends ElementId>, CloseableIterable<? extends Element>>,
        MultiInput<ElementId>,
        SeededGraphFilters,
        SeedMatching,
        Options {
    private SeedMatchingType seedMatching;
    private View view;
    private IncludeIncomingOutgoingType inOutType;
    private DirectedType directedType;
    private Iterable<? extends ElementId> input;
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
    public Iterable<? extends ElementId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends ElementId> input) {
        this.input = input;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @Override
    public Object[] createInputArray() {
        return MultiInput.super.createInputArray();
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

    public static class Builder extends Operation.BaseBuilder<GetElements, Builder>
            implements InputOutput.Builder<GetElements, Iterable<? extends ElementId>, CloseableIterable<? extends Element>, Builder>,
            MultiInput.Builder<GetElements, ElementId, Builder>,
            SeededGraphFilters.Builder<GetElements, Builder>,
            SeedMatching.Builder<GetElements, Builder>,
            Options.Builder<GetElements, Builder> {
        public Builder() {
            super(new GetElements());
        }
    }
}
