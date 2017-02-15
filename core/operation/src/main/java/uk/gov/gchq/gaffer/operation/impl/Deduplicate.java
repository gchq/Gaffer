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
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableOperation;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Deduplicate</code> operation takes in an {@link Iterable} of items
 * and removes duplicates.
 *
 * @see Deduplicate.Builder
 */
public class Deduplicate<T> extends AbstractGetIterableOperation<T, T> {
    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableObj();
    }

    public abstract static class BaseBuilder<T, CHILD_CLASS extends BaseBuilder<T, ?>>
            extends AbstractOperation.BaseBuilder<Deduplicate<T>, CloseableIterable<T>, CloseableIterable<T>, CHILD_CLASS> {

        public BaseBuilder() {
            super(new Deduplicate<T>());
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS input(final Iterable<T> input) {
            return super.input(new WrappedCloseableIterable<T>(input));
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS input(final CloseableIterable<T> input) {
            return super.input(input);
        }
    }

    public static final class Builder<T> extends BaseBuilder<T, Builder<T>> {

        @Override
        protected Builder<T> self() {
            return this;
        }
    }
}
