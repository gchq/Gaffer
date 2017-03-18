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

package uk.gov.gchq.koryphe.tuple.predicate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.gov.gchq.koryphe.predicate.AdaptedPredicate;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import java.util.function.Predicate;

/**
 * A <code>TuplePredicate</code> validates input {@link Tuple}s by applying a
 * {@link Predicate} to the tuple values.
 *
 * @param <R> The type of reference used by tuples.
 */
public class TupleAdaptedPredicate<R, FI> extends AdaptedPredicate<Tuple<R>, FI> {
    /**
     * Default constructor - for serialisation.
     */
    public TupleAdaptedPredicate() {
        setInputAdapter(new TupleInputAdapter<>());
    }

    @SafeVarargs
    public TupleAdaptedPredicate(final Predicate<FI> function, final R... selection) {
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
    }

    @JsonIgnore
    @Override
    public TupleInputAdapter<R, FI> getInputAdapter() {
        return (TupleInputAdapter<R, FI>) super.getInputAdapter();
    }
}
