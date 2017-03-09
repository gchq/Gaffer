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

package uk.gov.gchq.koryphe.tuple.function;

import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.mask.TupleMask;
import java.util.function.Function;

/**
 * A <code>TupleFunction</code> transforms input {@link Tuple}s by applying a
 * {@link Function} to the tuple values.
 * @param <R> The type of reference used by tuples.
 */
public class TupleFunction<R, I, O> extends TupleInputOutputFunction<R, I, O, Function<I, O>> implements Function<Tuple<R>, Tuple<R>> {
    /**
     * Default constructor - for serialisation.
     */
    public TupleFunction() {}

    public TupleFunction(TupleMask<R, I> selection, Function<I, O> function, TupleMask<R, O> projection) {
        super(selection, function, projection);
    }

    /**
     * Transform an input tuple.
     * @param input Input tuple
     */
    @Override
    public Tuple<R> apply(final Tuple<R> input) {
        if (input == null) {
            return null;
        } else {
            projection.setContext(input);
            return projection.project(function.apply(selection.select(input)));
        }
    }
}
