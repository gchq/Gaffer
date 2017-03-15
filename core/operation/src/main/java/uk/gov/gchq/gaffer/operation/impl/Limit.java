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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.IterableInput;
import uk.gov.gchq.gaffer.operation.io.IterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Limit</code> operation takes in an {@link Iterable} of items
 * and limits the iterable to a given number of items. It simply wraps the input
 * iterable in a {@link uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable} so
 * the data is not stored in memory.
 *
 * @see Limit.Builder
 */
public class Limit<T> implements
        Operation,
        IterableInput<T>,
        IterableOutput<T> {
    protected Integer resultLimit;
    private Iterable<T> input;

    public Limit() {
    }

    public Limit(final Integer resultLimit) {
        this.resultLimit = resultLimit;
    }

    public Integer getResultLimit() {
        return resultLimit;
    }

    public void setResultLimit(final Integer resultLimit) {
        this.resultLimit = resultLimit;
    }

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
            extends Operation.BaseBuilder<Limit<T>, Builder<T>>
            implements IterableInput.Builder<Limit<T>, T, Builder<T>>, IterableOutput.Builder<Limit<T>, T, Builder<T>> {
        public Builder() {
            super(new Limit<>());
        }

        public Builder<T> resultLimit(final Integer resultLimit) {
            _getOp().setResultLimit(resultLimit);
            return _self();
        }
    }
}
