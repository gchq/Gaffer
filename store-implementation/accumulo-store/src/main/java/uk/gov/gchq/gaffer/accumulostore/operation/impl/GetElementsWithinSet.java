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
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * Retrieves {@link uk.gov.gchq.gaffer.data.element.Edge}s where both ends are in a given
 * set and/or {@link uk.gov.gchq.gaffer.data.element.Entity}s where the vertex is in the
 * set.
 **/
@JsonPropertyOrder(value = {"class", "input", "view"}, alphabetic = true)
@Since("1.0.0")
@Summary("Gets edges with both vertices in a given set and entities with vertices in a given set")
public class GetElementsWithinSet implements
        InputOutput<Iterable<? extends EntityId>, CloseableIterable<? extends Element>>,
        MultiEntityIdInput,
        GraphFilters {
    private View view;
    private DirectedType directedType;
    private Iterable<? extends EntityId> input;
    private Map<String, String> options;

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
    public GetElementsWithinSet shallowClone() {
        return new GetElementsWithinSet.Builder()
                .view(view)
                .directedType(directedType)
                .input(input)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetElementsWithinSet, Builder>
            implements InputOutput.Builder<GetElementsWithinSet, Iterable<? extends EntityId>, CloseableIterable<? extends Element>, Builder>,
            MultiEntityIdInput.Builder<GetElementsWithinSet, Builder>,
            GraphFilters.Builder<GetElementsWithinSet, Builder> {
        public Builder() {
            super(new GetElementsWithinSet());
        }
    }
}
