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

import gaffer.operation.AbstractOperation;

/**
 * A <code>Deduplicate</code> operation takes in an {@link Iterable} of items
 * and removes duplicates.
 *
 * @see Deduplicate.Builder
 */
public class Deduplicate<T> extends AbstractOperation<Iterable<T>, Iterable<T>> {

    public static class Builder<T> extends AbstractOperation.Builder<Deduplicate<T>, Iterable<T>, Iterable<T>> {

        public Builder() {
            super(new Deduplicate<T>());
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see gaffer.operation.Operation#setInput(Object)
         */
        protected Builder<T> input(final Iterable<T> input) {
            return (Builder<T>) super.input(input);
        }

        @Override
        public Builder<T> option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
