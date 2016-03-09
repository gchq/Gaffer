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

/**
 * A {@link gaffer.tuple.handler.TupleHandler} that selects and projects the values into/out of a
 * {@link gaffer.tuple.Tuple} using a single reference.
 * @param <R> The type of reference used to select from and project into tuples.
 */
public class SingleReferenceHandler<R> implements TupleHandler<R> {
    private R reference;

    /**
     * Create a <code>SingleReferenceTupleHandler</code> with the given reference.
     * @param reference Value reference.
     */
    public SingleReferenceHandler(final R reference) {
        this.reference = reference;
    }

    /**
     * Select the value from a source {@link gaffer.tuple.Tuple} using this handler's reference.
     * @param source Source {@link gaffer.tuple.Tuple}.
     * @return Selected value.
     */
    public Object select(final Tuple<R> source) {
        return source.get(reference);
    }

    /**
     * Project a value into a target {@link gaffer.tuple.Tuple} using this handler's reference.
     * @param target Target {@link gaffer.tuple.Tuple}.
     * @param value Value to project.
     */
    public void project(final Tuple<R> target, final Object value) {
        target.put(reference, value);
    }

    /**
     * @return Value reference.
     */
    public R getReference() {
        return reference;
    }
}
