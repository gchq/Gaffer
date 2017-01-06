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
 * A <code>Tuple</code> provides a map-like interface to any data structure.
 *
 * @param <R> The type of reference used by the underlying data structure to access data values.
 */
public interface Tuple<R> {
    /**
     * @param reference Data item reference.
     * @return Data item corresponding to reference.
     */
    Object get(final R reference);

    /**
     * @param reference Data item reference.
     * @param value     Data item value to set on the underlying data structure.
     */
    void put(final R reference, final Object value);
}
