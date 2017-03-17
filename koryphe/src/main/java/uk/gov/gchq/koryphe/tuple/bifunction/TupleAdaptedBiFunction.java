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

package uk.gov.gchq.koryphe.tuple.bifunction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.gov.gchq.koryphe.bifunction.AdaptedBiFunction;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleOutputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleReverseOutputAdapter;
import java.util.function.BiFunction;

/**
 * A <code>TupleAdaptedBiFunction</code> aggregates {@link Tuple}s by applying a
 * {@link BiFunction} to aggregate the tuple values. Projects aggregated values into a
 * single output {@link Tuple}, which will be the second tuple supplied as input.
 *
 * @param <R> The type of reference used by tuples.
 */
public class TupleAdaptedBiFunction<R, FI, FO> extends AdaptedBiFunction<Tuple<R>, FI, FO, Tuple<R>> {
    public TupleAdaptedBiFunction() {
        setInputAdapter(new TupleInputAdapter<>());
        setOutputAdapter(new TupleOutputAdapter<>());
        setReverseOutputAdapter(new TupleReverseOutputAdapter<>());
    }

    @SafeVarargs
    public TupleAdaptedBiFunction(BiFunction<FI, FO, FO> function, R... selection) {
        this();
        setFunction(function);
        setSelection(selection);
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
    public TupleInputAdapter<R, FI> getInputAdapter() {
        return (TupleInputAdapter<R, FI>) super.getInputAdapter();
    }

    @JsonIgnore
    @Override
    public TupleOutputAdapter<R, FO> getOutputAdapter() {
        return (TupleOutputAdapter<R, FO>) super.getOutputAdapter();
    }

    @JsonIgnore
    @Override
    public TupleReverseOutputAdapter<R, FO> getReverseOutputAdapter() {
        return (TupleReverseOutputAdapter<R, FO>) super.getReverseOutputAdapter();
    }
}
