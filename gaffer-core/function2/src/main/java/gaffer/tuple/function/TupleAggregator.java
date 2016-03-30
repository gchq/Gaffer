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

import com.fasterxml.jackson.annotation.JsonIgnore;
import gaffer.function2.StatefulFunction;
import gaffer.function2.Aggregator;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * A <code>TupleAggregator</code> aggregates {@link gaffer.tuple.Tuple}s by applying
 * {@link gaffer.function2.StatefulFunction}>s to aggregate the tuple values. Projects
 * aggregated values into a single output {@link gaffer.tuple.Tuple}, which, if not
 * externally specified, will be the first tuple supplied as input.
 * @param <R> The type of reference used by tuples.
 */
public class TupleAggregator<F extends StatefulFunction, R> extends Aggregator<Tuple<R>> {
    private List<FunctionContext<F, R>> functions;
    @JsonIgnore
    private Tuple<R> outputTuple;

    /**
     * Default constructor - for serialisation.
     */
    public TupleAggregator() { }

    /**
     * Create a <code>TupleAggregator</code> that applies the given functions.
     * @param functions {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public TupleAggregator(final List<FunctionContext<F, R>> functions) {
        setFunctions(functions);
        outputTuple = null;
    }

    /**
     * @param functions {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public void setFunctions(final List<FunctionContext<F, R>> functions) {
        this.functions = functions;
    }

    /**
     * @return {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public List<FunctionContext<F, R>> getFunctions() {
        return functions;
    }

    /**
     * @param function {@link gaffer.function2.StatefulFunction} to aggregate tuple values.
     */
    public void addFunction(final FunctionContext<F, R> function) {
        if (functions == null) {
            functions = new ArrayList<FunctionContext<F, R>>();
        }
        functions.add(function);
    }

    /**
     * @param outputTuple Tuple that the aggregated values should be projected into.
     */
    public void setOutputTuple(final Tuple<R> outputTuple) {
        this.outputTuple = outputTuple;
    }

    /**
     * Initialise this <code>TupleAggregator</code> and all of it's aggregation functions.
     */
    @Override
    public void init() {
        outputTuple = null;
        if (functions != null) {
            for (FunctionContext<F, R> function : functions) {
                function.getFunction().init();
            }
        }
    }

    /**
     * Aggregate an input tuple.
     * @param input Input tuple
     */
    @Override
    public void aggregate(final Tuple<R> input) {
        if (functions != null) {
            if (outputTuple == null) {
                setOutputTuple(input);
            }
            for (FunctionContext<F, R> function : functions) {
                function.getFunction().execute(function.select(input));
            }
        }
    }

    /**
     * Aggregate a group of input tuples to produce an output tuple.
     * @param group Input tuples.
     * @return Output tuple.
     */
    public Tuple<R> aggregateGroup(final Iterable<Tuple<R>> group) {
        init();
        for (Tuple<R> input : group) {
            aggregate(input);
        }
        return state();
    }

    /**
     * @return Project the current state of the functions into the output tuple.
     */
    @Override
    public Tuple<R> state() {
        if (outputTuple != null) {
            for (FunctionContext<F, R> function : functions) {
                function.project(outputTuple, function.getFunction().state());
            }
        }
        return outputTuple;
    }

    @Override
    public boolean assignableFrom(final Object schemaTuple) {
        return TupleFunctionValidator.assignableFrom(functions, schemaTuple);
    }

    @Override
    public boolean assignableTo(final Object schemaTuple) {
        return TupleFunctionValidator.assignableTo(functions, schemaTuple);
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
