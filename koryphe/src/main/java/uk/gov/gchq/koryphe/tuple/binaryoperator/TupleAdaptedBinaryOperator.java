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

package uk.gov.gchq.koryphe.tuple.binaryoperator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.gov.gchq.koryphe.binaryoperator.AdaptedBinaryOperator;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleOutputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleReverseOutputAdapter;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

/**
 * A <code>TupleCombiner</code> aggregates {@link Tuple}s by applying a
 * {@link BiFunction} to aggregate the tuple values. Projects aggregated values into a
 * single output {@link Tuple}, which will be the first tuple supplied as input.
 *
 * @param <R> The type of reference used by tuples.
 */
public class TupleAdaptedBinaryOperator<R, FT> extends AdaptedBinaryOperator<Tuple<R>, FT> {
    public TupleAdaptedBinaryOperator() {
        setInputAdapter(new TupleInputAdapter<>());
        setOutputAdapter(new TupleOutputAdapter<>());
        setReverseOutputAdapter(new TupleReverseOutputAdapter<>());
    }

    public TupleAdaptedBinaryOperator(BinaryOperator<FT> function) {
        this();
        setFunction(function);
    }

    @SafeVarargs
    public TupleAdaptedBinaryOperator(BinaryOperator<FT> function, R... selection) {
        this(function);
        setSelection(selection);
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

    public R[] getSelection() {
        return getInputAdapter().getSelection();
    }

    @SafeVarargs
    public final void setSelection(final R... selection) {
        getInputAdapter().setSelection(selection);
        getOutputAdapter().setProjection(selection);
        getReverseOutputAdapter().setProjection(selection);
    }

    @JsonIgnore
    @Override
    public TupleInputAdapter<R, FT> getInputAdapter() {
        return (TupleInputAdapter<R, FT>) super.getInputAdapter();
    }

    @JsonIgnore
    @Override
    public TupleOutputAdapter<R, FT> getOutputAdapter() {
        return (TupleOutputAdapter<R, FT>) super.getOutputAdapter();
    }

    @JsonIgnore
    @Override
    public TupleReverseOutputAdapter<R, FT> getReverseOutputAdapter() {
        return (TupleReverseOutputAdapter<R, FT>) super.getReverseOutputAdapter();
    }
}
