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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.IterableInputIterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>ToVertices</code> converts {@link uk.gov.gchq.gaffer.data.element.id.ElementId}s
 * into vertices.
 */
public class ToVertices implements
        Operation,
        IterableInputIterableOutput<ElementId, Object> {
    private Iterable<ElementId> input;


    @Override
    public Iterable<ElementId> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<ElementId> input) {
        this.input = input;
    }

    @Override
    public TypeReference<CloseableIterable<Object>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableObj();
    }

    public static final class Builder
            extends BaseBuilder<ToVertices, ToVertices.Builder>
            implements IterableInputIterableOutput.Builder<ToVertices, ElementId, Object, ToVertices.Builder> {
        public Builder() {
            super(new ToVertices());
        }
    }
}
