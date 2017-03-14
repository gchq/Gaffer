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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.IterableInput;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Deduplicate</code> operation takes in an {@link Iterable} of items
 * and removes duplicates.
 *
 * @see Deduplicate.Builder
 */
public class Deduplicate<T> implements Operation, IterableInput<T>, IterableOutput<T> {
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
    public TypeReference<CloseableIterable<T>> getOutputTypeReference() {
        return TypeReferenceImpl.createCloseableIterableT();
    }

    public static final class Builder<T>
            extends Operation.BaseBuilder<Deduplicate<T>, Builder<T>>
            implements IterableInput.Builder<Deduplicate<T>, T, Builder<T>>,
            IterableOutput.Builder<Deduplicate<T>, T, Builder<T>> {
        public Builder() {
            super(new Deduplicate<>());
        }
    }
}
