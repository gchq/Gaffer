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
import java.util.function.Predicate;

/**
 * A <code>TuplePredicate</code> validates input {@link Tuple}s by applying a
 * {@link Predicate} to the tuple values.
 *
 * @param <R> The type of reference used by tuples.
 */
public class TuplePredicate<R, I> extends TupleInputPredicate<R, I, Predicate<I>> implements Predicate<Tuple<R>> {
    /**
     * Default constructor - for serialisation.
     */
    public TuplePredicate() {
    }

    public TuplePredicate(TupleMask<R, I> selection, Predicate<I> function) {
        super(selection, function);
    }

    @Override
    public boolean test(Tuple<R> input) {
        return function.test(selection.select(input));
    }
}
