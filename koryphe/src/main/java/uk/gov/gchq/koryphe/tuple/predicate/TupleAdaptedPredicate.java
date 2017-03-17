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
    public TupleAdaptedPredicate(Predicate<FI> function, R... selection) {
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

    public static class Builder<R, FI> {
        private boolean selected = false;

        private final TupleAdaptedPredicate<R, FI> tuplePredicate;
        private boolean executed = false;

        public Builder() {
            this(new TupleAdaptedPredicate<>());
        }

        protected Builder(final TupleAdaptedPredicate<R, FI> tuplePredicate) {
            this.tuplePredicate = tuplePredicate;
        }

        public Builder execute(final Predicate<FI> function) {
            if (!executed) {
                tuplePredicate.setFunction(function);
                executed = true;
            } else {
                throw new IllegalStateException("Function has already been set");
            }
            return this;
        }

        @SafeVarargs
        public final Builder<R, FI> select(final R... selection) {
            if (!selected) {
                tuplePredicate.setSelection(selection);
                selected = true;
            } else {
                throw new IllegalStateException("Selection has already been set");
            }
            return this;
        }

        public TupleAdaptedPredicate<R, FI> build() {
            return getTuplePredicate();
        }

        protected boolean isExecuted() {
            return executed;
        }

        protected boolean isSelected() {
            return selected;
        }

        protected TupleAdaptedPredicate<R, FI> getTuplePredicate() {
            return tuplePredicate;
        }
    }
}
