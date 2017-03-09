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
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

/**
 * A <code>TupleCombiner</code> aggregates {@link Tuple}s by applying a
 * {@link BiFunction} to aggregate the tuple values. Projects aggregated values into a
 * single output {@link Tuple}, which will be the first tuple supplied as input.
 *
 * @param <R> The type of reference used by tuples.
 */
public class TupleBiFunction<R, I, O> extends TupleInputOutputBiFunction<R, I, O, BiFunction<I, O, O>> implements BinaryOperator<Tuple<R>> {
    /**
     * Default constructor - for serialisation.
     */
    public TupleBiFunction() {
    }

    public TupleBiFunction(TupleMask<R, I> selection, BiFunction<I, O, O> function, TupleMask<R, O> projection) {
        super(selection, function, projection);
    }

    /**
     * Aggregate an input tuple with the current state tuple.
     *
     * @param input Input tuple
     * @param state State tuple
     */
    @Override
    public Tuple<R> apply(final Tuple<R> input, final Tuple<R> state) {
        if (input == null) {
            return state;
        } else {
            Tuple<R> currentStateTuple;
            O currentState = null;
            if (state == null) {
                currentStateTuple = input;
            } else {
                currentStateTuple = state;
                currentState = projection.select(state);
            }
            projection.setContext(currentStateTuple);
            return projection.project(function.apply(selection.select(input), currentState));
        }
    }

    /**
     * Aggregate a group of input tuples to produce an output tuple.
     *
     * @param group Input tuples.
     * @return Output tuple.
     */
    public Tuple<R> applyGroup(final Iterable<Tuple<R>> group) {
        Tuple<R> state = null;
        for (Tuple<R> input : group) {
            state = apply(input, state);
        }
        return state;
    }
}
