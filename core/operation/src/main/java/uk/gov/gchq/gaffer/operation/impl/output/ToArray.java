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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.IterableInputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class ToArray<T> implements
        Operation,
        IterableInputOutput<T, T[]> {
    private Iterable<T> input;

    @Override
    public Iterable<T> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<T> input) {
        this.input = input;
    }

    @Override
    public TypeReference<T[]> getOutputTypeReference() {
        return new TypeReferenceImpl.Array();
    }

    public static final class Builder<T>
            extends BaseBuilder<ToArray<T>, ToArray.Builder<T>>
            implements IterableInputOutput.Builder<ToArray<T>, T, T[], ToArray.Builder<T>> {
        public Builder() {
            super(new ToArray<>());
        }
    }
}
