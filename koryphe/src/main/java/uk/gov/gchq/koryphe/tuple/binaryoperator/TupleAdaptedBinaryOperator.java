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

import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleOutputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleReverseOutputAdapter;
import uk.gov.gchq.koryphe.tuple.bifunction.TupleAdaptedBiFunction;
import java.util.function.BinaryOperator;

/**
 * A <code>TupleAdaptedBinaryOperator</code> aggregates {@link uk.gov.gchq.koryphe.tuple.Tuple}s by applying a
 * {@link BinaryOperator} to aggregate the tuple values. Projects aggregated values into a
 * single output {@link uk.gov.gchq.koryphe.tuple.Tuple}, which will be the second tuple supplied as input.
 *
 * @param <R> The type of reference used by tuples.
 */
public class TupleAdaptedBinaryOperator<R, FT> extends TupleAdaptedBiFunction<R, FT, FT> {
    public TupleAdaptedBinaryOperator() {
        setInputAdapter(new TupleInputAdapter<>());
        setOutputAdapter(new TupleOutputAdapter<>());
        setReverseOutputAdapter(new TupleReverseOutputAdapter<>());
    }

    @SafeVarargs
    public TupleAdaptedBinaryOperator(final BinaryOperator<FT> function, final R... selection) {
        this();
        setFunction(function);
        setSelection(selection);
    }

    @Override
    public BinaryOperator<FT> getFunction() {
        return (BinaryOperator<FT>) super.getFunction();
    }
}
