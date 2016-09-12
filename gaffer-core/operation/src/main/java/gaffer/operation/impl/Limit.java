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

package gaffer.operation.impl;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.WrappedCloseableIterable;
import gaffer.operation.AbstractGetOperation;

/**
 * A <code>Truncate</code> operation takes in an {@link Iterable} of items
 * and limits the iterable to a given number of items. It simply wraps the input
 * iterable in a {@link gaffer.commonutil.iterable.LimitedCloseableIterable} so
 * the data is not stored in memory.
 *
 * @see Limit.Builder
 */
public class Limit<T> extends AbstractGetOperation<T, T> {

    public static class Builder<T> extends AbstractGetOperation.Builder<Limit<T>, T, T> {

        public Builder() {
            super(new Limit<T>());
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see gaffer.operation.Operation#setInput(Object)
         */
        public Builder<T> input(final Iterable<T> input) {
            return input(new WrappedCloseableIterable<>(input));
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see gaffer.operation.Operation#setInput(Object)
         */
        public Builder<T> input(final CloseableIterable<T> input) {
            return (Builder<T>) super.input(input);
        }

        @Override
        public Builder<T> limitResults(final Integer resultLimit) {
            return (Builder<T>) super.limitResults(resultLimit);
        }

        @Override
        public Builder<T> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
