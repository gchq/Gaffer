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

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableInput;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * This returns all data between the provided
 * {@link uk.gov.gchq.gaffer.operation.data.ElementSeed}s.
 */
public class GetElementsInRanges<I_ITEM extends Pair<? extends ElementSeed>, E extends Element>
        implements Operation,
        SeededGraphFilters,
        IterableInput<I_ITEM>,
        IterableOutput<E> {

    private Iterable<I_ITEM> input;
    private IncludeIncomingOutgoingType inOutType;
    private View view;
    private DirectedType directedType;

    @Override
    public Iterable<I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<I_ITEM> input) {
        this.input = input;
    }

    @Override
    public TypeReference<CloseableIterable<E>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.CloseableIterableElement();
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

    public static class Builder<I_ITEM extends Pair<? extends ElementSeed>, E extends Element>
            extends Operation.BaseBuilder<GetElementsInRanges<I_ITEM, E>, Builder<I_ITEM, E>>
            implements IterableInput.Builder<GetElementsInRanges<I_ITEM, E>, I_ITEM, Builder<I_ITEM, E>> {
        protected Builder() {
            super(new GetElementsInRanges<>());
        }
    }
}
