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

package uk.gov.gchq.koryphe.tuple;

/**
 * A <code>Tuple</code> provides a map-like interface to any data structure.
 * @param <R> The type of reference used by the underlying data structure to access data values.
 */
public interface Tuple<R> extends Iterable<Object> {
    /**
     * Put a value into this <code>Tuple</code> with the given reference.
     * @param reference Value reference.
     * @param value Value to put.
     */
    void put(R reference, Object value);

    /**
     * Get a value from this <code>Tuple</code> with the given reference.
     * @param reference Value reference.
     * @return Value or null if not present.
     */
    Object get(R reference);

    /**
     * @return Values in this <code>Tuple</code>.
     */
    Iterable<Object> values();
}
