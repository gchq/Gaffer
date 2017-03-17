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
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.io.IterableInputIterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Collections;
import java.util.Map;

/**
 * An <code>GetAdjacentVertices</code> operation will return the
 * vertices at the opposite end of connected edges to a provided seed vertices
 *
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds.Builder
 */
public class GetAdjacentIds implements
        Operation,
        IterableInputIterableOutput<EntityId, EntityId>,
        SeededGraphFilters,
        Options {
    private View view;
    private Iterable<EntityId> input;
    private DirectedType directedType;
    private Map<String, String> options;
    private IncludeIncomingOutgoingType inOutType;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    public Object[] createInputArray() {
        return IterableInputIterableOutput.super.createInputArray();
    }

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        if (null != view && view.hasEntities()) {
            this.view = (new View.Builder()
                    .merge(view)
                    .entities(Collections.emptyMap())
                    .build());
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
    public Iterable<EntityId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<EntityId> input) {
        this.input = input;
    }

    @Override
    public TypeReference<CloseableIterable<EntityId>> getOutputTypeReference() {
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

    public static class Builder extends Operation.BaseBuilder<GetAdjacentIds, Builder>
            implements IterableInputIterableOutput.Builder<GetAdjacentIds, EntityId, EntityId, Builder>,
            SeededGraphFilters.Builder<GetAdjacentIds, Builder>,
            Options.Builder<GetAdjacentIds, Builder> {
        public Builder() {
            super(new GetAdjacentIds());
        }
    }
}
