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

package gaffer.tuple.function;

import gaffer.function2.StatefulFunction;
import gaffer.function2.Aggregator;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.function.context.FunctionContexts;

/**
 * A <code>TupleAggregator</code> aggregates {@link gaffer.tuple.Tuple}s by applying
 * {@link gaffer.function2.StatefulFunction}>s to aggregate the tuple values. Projects aggregated values into a single
 * output {@link gaffer.tuple.Tuple}, which will be the first tuple supplied as input.
 * @param <R> The type of reference used by tuples.
 */
public class TupleAggregator<F extends StatefulFunction, R> extends Aggregator<Tuple<R>> {
    private FunctionContexts<F, R> functions;

    /**
     * Default constructor - for serialisation.
     */
    public TupleAggregator() { }

    /**
     * Create a <code>TupleAggregator</code> that applies the given functions.
     * @param functions {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public TupleAggregator(final FunctionContexts<F, R> functions) {
        setFunctions(functions);
    }

    /**
     * @param functions {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public void setFunctions(final FunctionContexts<F, R> functions) {
        this.functions = functions;
    }

    /**
     * @return {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public FunctionContexts<F, R> getFunctions() {
        return functions;
    }

    /**
     * @param function {@link gaffer.function2.StatefulFunction} to aggregate tuple values.
     */
    public void addFunction(final FunctionContext<F, R> function) {
        if (functions == null) {
            functions = new FunctionContexts<F, R>();
        }
        functions.add(function);
    }

    /**
     * Aggregate an input tuple with the current state tuple.
     * @param input Input tuple
     * @param state State tuple
     */
    @Override
    public Tuple<R> execute(final Tuple<R> input, final Tuple<R> state) {
        Tuple<R> result = state != null ? state : input;
        if (functions != null) {
            for (FunctionContext<F, R> function : functions) {
                Object functionState = state == null ? null : function.getProjectionView().select(state);
                function.project(result, function.getFunction().execute(function.select(input), functionState));
            }
        }
        return result;
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

    @Override
    public boolean assignableFrom(final Object schemaTuple) {
        return functions.assignableFrom(schemaTuple);
    }

    @Override
    public boolean assignableTo(final Object schemaTuple) {
        return functions.assignableTo(schemaTuple);
    }

    /**
     * @return New <code>TupleAggregator</code> with new {@link gaffer.function2.StatefulFunction}s.
     */
    public TupleAggregator<F, R> copy() {
        TupleAggregator<F, R> copy = new TupleAggregator<F, R>();
        for (FunctionContext<F, R> function : this.functions) {
            copy.addFunction(function.copy());
        }
        return copy;
    }
}
