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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Count</code> operation counts how many items there are in the provided {@link CloseableIterable}.
 *
 * @see Count.Builder
 */
public class Count<T> extends AbstractOperation<CloseableIterable<T>, Long> {

    public Count() {
    }

    /**
     * @return the input {@link CloseableIterable} of objects of class <code>T</code> to be counted.
     */
    public CloseableIterable<T> getItems() {
        return getInput();
    }

    /**
     * @param items the input {@link Iterable} of objects of class <code>T</code> to be counted.
     */
    public void setItems(final CloseableIterable<T> items) {
        setInput(items);
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Long();
    }

    public abstract static class BaseBuilder<T, CHILD_CLASS extends BaseBuilder<T, ?>>
            extends AbstractOperation.BaseBuilder<Count<T>, CloseableIterable<T>, Long, CHILD_CLASS> {

        public BaseBuilder() {
            super(new Count());
        }

        /**
         * @param items the input {@link CloseableIterable} of objects of class <code>T</code> to be set on the operation.
         * @return this Builder
         * @see Count#setItems(CloseableIterable)
         */
        public CHILD_CLASS elements(final CloseableIterable<T> items) {
            op.setInput(items);
            return self();
        }
    }

    public static final class Builder<T> extends BaseBuilder<T, Builder<T>> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
