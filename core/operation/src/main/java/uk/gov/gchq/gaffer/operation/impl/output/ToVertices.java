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
package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>ToVertices</code> takes an {@link java.lang.Iterable} of
 * {@link uk.gov.gchq.gaffer.data.element.id.ElementId}s and converts them into
 * vertices.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.output.ToVertices.Builder
 */
public class ToVertices implements
        Operation,
        InputOutput<Iterable<? extends ElementId>, Iterable<? extends Object>>,
        MultiInput<ElementId> {

    private Iterable<? extends ElementId> input;
    private EdgeVertices edgeVertices = EdgeVertices.NONE;

    @Override
    public Iterable<? extends ElementId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends ElementId> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableObj();
    }

    public EdgeVertices getEdgeVertices() {
        return edgeVertices;
    }

    public void setEdgeVertices(final EdgeVertices edgeVertices) {
        this.edgeVertices = edgeVertices;
    }

    public enum EdgeVertices {
        NONE,
        SOURCE,
        DESTINATION,
        BOTH;
    }

    public static final class Builder
            extends BaseBuilder<ToVertices, Builder>
            implements InputOutput.Builder<ToVertices, Iterable<? extends ElementId>, Iterable<? extends Object>, Builder>,
            MultiInput.Builder<ToVertices, ElementId, Builder> {
        public Builder() {
            super(new ToVertices());
        }

        public Builder edgeVertices(final EdgeVertices edgeVertices) {
            _getOp().setEdgeVertices(edgeVertices);
            return _self();
        }
    }
}
