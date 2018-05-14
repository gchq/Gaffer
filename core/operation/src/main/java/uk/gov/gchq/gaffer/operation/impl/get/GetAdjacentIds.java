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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiEntityIdInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Collections;
import java.util.Map;

/**
 * A {@code GetAdjacentIds} operation will return the
 * vertex at the opposite end of connected edges to a provided seed vertex.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "view"}, alphabetic = true)
@Since("1.0.0")
@Summary("Performs a single hop down related edges")
public class GetAdjacentIds implements
        InputOutput<Iterable<? extends EntityId>, CloseableIterable<? extends EntityId>>,
        MultiEntityIdInput,
        SeededGraphFilters {
    private View view;
    private Iterable<? extends EntityId> input;
    private DirectedType directedType;
    private Map<String, String> options;
    private IncludeIncomingOutgoingType inOutType;

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        if (null != view && view.hasEntities()) {
            if (view.hasEntityFilters()) {
                throw new IllegalArgumentException("View should not have entities with filters.");
            }
            this.view = new View.Builder()
                    .merge(view)
                    .entities(Collections.emptyMap())
                    .build();
        } else {
            this.view = view;
        }
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
    public TypeReference<CloseableIterable<? extends EntityId>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableEntityId();
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
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return inOutType;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType inOutType) {
        this.inOutType = inOutType;
    }

    @Override
    public GetAdjacentIds shallowClone() {
        return new GetAdjacentIds.Builder()
                .view(view)
                .input(input)
                .directedType(directedType)
                .options(options)
                .inOutType(inOutType)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<GetAdjacentIds, Builder>
            implements InputOutput.Builder<GetAdjacentIds, Iterable<? extends EntityId>, CloseableIterable<? extends EntityId>, Builder>,
            MultiEntityIdInput.Builder<GetAdjacentIds, Builder>,
            SeededGraphFilters.Builder<GetAdjacentIds, Builder> {
        public Builder() {
            super(new GetAdjacentIds());
        }
    }
}
