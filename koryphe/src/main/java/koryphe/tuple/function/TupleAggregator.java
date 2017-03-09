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

package koryphe.tuple.function;

import koryphe.function.aggregate.Aggregator;
import koryphe.tuple.Tuple;
import koryphe.tuple.mask.TupleMask;

/**
 * A <code>TupleAggregator</code> aggregates {@link Tuple}s by applying a
 * {@link Aggregator} to aggregate the tuple values. Projects aggregated values into a
 * single output {@link Tuple}, which will be the first tuple supplied as input.
 * @param <R> The type of reference used by tuples.
 */
public class TupleAggregator<R, T> extends TupleInputFunction<R, T, T, Aggregator<T>> implements Aggregator<Tuple<R>> {
    /**
     * Default constructor - for serialisation.
     */
    public TupleAggregator() {}

    public TupleAggregator(TupleMask<R, T> selection, Aggregator<T> function) {
        super(selection, function);
    }

    /**
     * Aggregate an input tuple with the current state tuple.
     * @param input Input tuple
     * @param state State tuple
     */
    @Override
    public Tuple<R> execute(final Tuple<R> input, final Tuple<R> state) {
        if (input == null) {
            return state;
        } else {
            Tuple<R> currentStateTuple;
            T currentState = null;
            if (state == null) {
                currentStateTuple = input;
            } else {
                currentStateTuple = state;
                currentState = selection.select(state);
            }
            T output = function.execute(selection.select(input), currentState);
            selection.setContext(currentStateTuple);
            return selection.project(output);
        }
    }

    /**
     * Aggregate a group of input tuples to produce an output tuple.
     * @param group Input tuples.
     * @return Output tuple.
     */
    public Tuple<R> executeGroup(final Iterable<Tuple<R>> group) {
        Tuple<R> state = null;
        for (Tuple<R> input : group) {
            state = execute(input, state);
        }
        return state;
    }
}
