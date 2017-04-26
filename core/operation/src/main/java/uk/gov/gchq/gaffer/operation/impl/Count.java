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
package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Count</code> operation counts how many items there are in the provided {@link Iterable}.
 *
 * @see Count.Builder
 */
public class Count<T> implements
        Operation,
        InputOutput<Iterable<? extends T>, Long>,
        MultiInput<T> {
    private Iterable<? extends T> input;

    @Override
    public Iterable<? extends T> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends T> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Long> getOutputTypeReference() {
        return new TypeReferenceImpl.Long();
    }

    public static final class Builder<T>
            extends Operation.BaseBuilder<Count<T>, Builder<T>>
            implements InputOutput.Builder<Count<T>, Iterable<? extends T>, Long, Builder<T>>,
            MultiInput.Builder<Count<T>, T, Builder<T>> {
        public Builder() {
            super(new Count<>());
        }
    }
}
