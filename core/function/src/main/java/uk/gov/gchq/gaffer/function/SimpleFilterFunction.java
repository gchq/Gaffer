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

package uk.gov.gchq.gaffer.function;

/**
 * An <code>SimpleFilterFunction</code> is a {@link FilterFunction} that
 * takes a single input object.
 *
 * @param <T> the type of input
 */
public abstract class SimpleFilterFunction<T> extends FilterFunction {
    @Override
    public boolean isValid(final Object[] input) {
        if (null == input || 1 != input.length) {
            throw new IllegalArgumentException("Expected an input array of length 1");
        }

        try {
            return isValid((T) input[0]);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Input does not match parametrised type", e);
        }
    }

    protected abstract boolean isValid(final T input);
}
