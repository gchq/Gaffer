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

package gaffer.tuple.handler;

import gaffer.tuple.Tuple;

import java.util.List;

/**
 * A <code>TupleHandler</code> allows the selection and projection of tuple values.
 * @param <R> The type of reference used to select from and project into tuples.
 */
public interface TupleHandler<R> {
    /**
     * Select a value from the source {@link gaffer.tuple.Tuple}.
     * @param source Source tuple.
     * @return Selected value.
     */
    Object select(Tuple<R> source);

    /**
     * Project a value into the target {@link gaffer.tuple.Tuple}.
     * @param target Target tuple.
     * @param value Value to project.
     */
    void project(Tuple<R> target, Object value);

    /**
     * @return References used by this <code>TupleHandler</code>.
     */
    List<R> getReferences();
}
